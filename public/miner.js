const state = {
  ws: null,
  worker: null,
  mining: false,
  hashrate: 0,
  accepted: 0,
  rejected: 0,
  mode: 'standby',
  status: 'disconnected',
  workerReady: false,
  pendingConnect: false,
  reconnectTimer: null,
  reconnectAttempt: 0,     // backoff : 1 s, 2 s, 4 s ... 30 s max, remis à 0 après un login réussi
  lastMsgAt: 0,            // dernier message reçu du proxy (watchdog)
  currentJobId: null,
  currentJobSeq: 0,
  currentJobDiff: 0,
  lastJob: null,
  nicehash: false,
  shareEtaStart: 0,        // anchored on first non-zero hashrate + share-accepted
  shareEtaTotal: 0,        // SNAPSHOT of diff/hashrate at anchor time — stable for the duration of one share search
  datasetBuilt: false,     // light mode: true on first 'mode'; full: set by dataset_progress
  hashrateMax: 0,          // session-max for the htop-style hashrate bar
};

const params = new URLSearchParams(location.search);

// Mobile : mémoire limitée (iOS tue l'onglet très vite). On plafonne les
// threads et on interdit le mode Full (~2,5 Go).
const isMobile = /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent) ||
  (navigator.maxTouchPoints > 1 && /Macintosh/.test(navigator.userAgent));
const MAX_THREADS = isMobile ? 2 : 32;

// No SharedArrayBuffer (the page is not crossOriginIsolated, or ?sab=0 forces
// it for A/B): no pthreads, so mining runs as N single-thread workers on the
// randomx_st build (NoSabPool below), each in JIT'd light mode with its own
// 256 MiB cache (~300 MB per worker; ?threads=N caps it).
const noSab = params.get('sab') === '0' || window.crossOriginIsolated !== true;
// ?fb_full=K (0..2, no SAB only): K of those workers become full-dataset
// replicas (a private ~2.3 GB dataset each), built cooperatively by all workers
// (fb_full.js); a worker that can't allocate one stays in light mode.
const fbFull = (noSab && !isMobile) ? Math.max(0, Math.min(2, Math.floor(Number(params.get('fb_full')) || 0))) : 0;
// Safe browser default: use light mode. Opt into full mode with ?full=1
// (jamais sur mobile). `let` : repli automatique en Light si le mode Full
// provoque des erreurs répétées (voir recoverWorker).
let fullMemory = !isMobile && !noSab && params.get('full') === '1' && params.get('light') !== '1';
// JIT defaults: on for all engines. The threaded-interpreter + V2-minimal
// (split_inner_dispatch) path landed in worker.js makes the JIT a clear win
// everywhere — Safari 560 H/s, Chrome 530 H/s, Firefox 550+ at 32T on M4
// Mini. The old Safari-only auto-disable (which existed because per-program
// dynamic-module compile cost exceeded runtime saving on JSC) is no longer
// relevant once the resident-module path is the default. Opt out with
// ?nojit=1 / ?jit=0.
let jitDecision;
if (params.get('jit') === '0' || params.get('nojit') === '1') {
  jitDecision = false;
} else {
  jitDecision = true;
}
const enableJit = jitDecision;
const profileCore = params.get('profile') === '1';
const requestedThreads = Number(params.get('threads'));
// Mode Full : par défaut autant de threads que de cœurs (4 max), pas 32.
const defaultThreads = fullMemory
  ? Math.max(1, Math.min(navigator.hardwareConcurrency || 2, 4))
  : 1;
let datasetThreads = Math.max(1, Math.min(MAX_THREADS, requestedThreads || defaultThreads));
// Dataset init thread count is decoupled from mining threads. The em-pthread
// pool is 32 (build.sh:PTHREAD_POOL_SIZE), per-item init is independent
// compute on a read-mostly cache, so oversubscription on hardware threads
// is safe and helps wall-clock when hardwareConcurrency<32. Users at
// ?threads=1 used to wait ~150s for init on M4 Mini; now ~26s. Override
// for benchmarking with ?init_threads=N.
let datasetInitThreads = Math.max(1, Math.min(MAX_THREADS,
  Number(params.get('init_threads')) || MAX_THREADS));

// ───── Paramètres de robustesse ──────────────────────────────────────
const RECONNECT_BASE_MS   = 1000;    // 1re tentative après 1 s
const RECONNECT_MAX_MS    = 30000;   // jamais plus de 30 s entre deux tentatives
const WS_SILENCE_MS       = 120000;  // aucun message du proxy pendant 2 min → on reconnecte
const WORKER_MAX_FAILURES = 4;       // au-delà : arrêt propre
const LOG_MAX_LINES       = 150;     // taille max du journal

const $ = (id) => document.getElementById(id);

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

function readLe(bytes, offset, len) {
  let value = 0n;
  for (let i = 0; i < len; i++) {
    value |= BigInt(bytes[offset + i]) << BigInt(i * 8);
  }
  return value;
}

function targetToDiff(targetHex) {
  const raw = hexToBytes(targetHex);
  let target = 0n;

  if (raw.length === 4) {
    const target32 = readLe(raw, 0, 4);
    if (target32 !== 0n) {
      target = 0xffffffffffffffffn / (0xffffffffn / target32);
    }
  } else if (raw.length === 8) {
    target = readLe(raw, 0, 8);
  } else if (raw.length >= 32) {
    target = readLe(raw, 24, 8);
  }

  return target ? Number(0xffffffffffffffffn / target) : 0;
}

function updateUI() {
  $('hashrateBar').innerHTML = renderHashrateBar(state.hashrate, state.hashrateMax)
    + ' ' + state.hashrate.toFixed(0).padStart(4, ' ') + ' H/s';
  $('hashrateMeta').textContent = state.hashrateMax > 0
    ? `max ${state.hashrateMax.toFixed(0)} H/s · session`
    : 'no samples yet';
  $('accepted').textContent = state.accepted;
  $('rejected').textContent = state.rejected;
  $('mode').textContent = state.mode;
  $('status').textContent = state.status;
  $('toggle').textContent = state.mining ? 'Stop' : 'Start';
  // Don't blow away .pulse — toggle only the .active modifier.
  $('toggle').classList.toggle('active', state.mining);
  updateMini();
}

// Pads numeric fields so digit-count changes don't shift the collapsed row.
function updateMini() {
  if (!$('miniEta')) return;
  // Time remaining to next share (negative = overdue, prefixed with +Ns).
  let etaText = '--';
  if (state.mining && state.shareEtaStart > 0 && state.shareEtaTotal > 0) {
    const remain = state.shareEtaTotal - (performance.now() - state.shareEtaStart) / 1000;
    etaText = remain >= 0
      ? Math.ceil(remain) + 's'
      : '+' + Math.ceil(-remain) + 's';
  }
  $('miniEta').textContent = etaText.padStart(5, ' ');
  $('miniShares').textContent = state.accepted + '/' + state.rejected;
  $('miniHashrate').textContent = state.hashrate.toFixed(0).padStart(4, ' ');
  $('miniToggle').textContent = state.mining ? 'Stop' : 'Start';
  $('miniToggle').classList.toggle('active', state.mining);
}

// htop-style hashrate bar: 40 cells, filled to round(current/max * 40), with
// the rightmost filled cell wrapped in .blink-cell so it pulses.
function renderHashrateBar(current, max) {
  if (!max || max <= 0 || !current || current <= 0) return '▱'.repeat(40);
  const ratio = Math.min(1, current / max);
  let n = Math.round(ratio * 40);
  if (n < 1) n = 1;
  if (n > 40) n = 40;
  return '▰'.repeat(n - 1) + '<span class="blink-cell">▰</span>' + '▱'.repeat(40 - n);
}

// 40-cell progress bar: ▰×n ▱×(40-n) NNN%. Tabular-nums in the CSS keeps the
// percent from jittering. Returns an HTML string (safe — content is fully
// controlled here). When opts.blinkOnOverflow is set and pct > 100, the last
// cell is wrapped in a .blink-cell span so CSS can pulse it.
function renderBar(pct, opts) {
  opts = opts || {};
  if (typeof pct !== 'number' || !isFinite(pct) || pct < 0) {
    return '▱'.repeat(40) + '  --%';
  }
  if (pct > 100 && opts.blinkOnOverflow) {
    const pctStr = Math.round(pct).toString().padStart(3, ' ') + '%';
    return '▰'.repeat(39) + '<span class="blink-cell">▰</span> ' + pctStr;
  }
  const p = Math.min(100, pct);
  const n = Math.round(p * 40 / 100);
  return '▰'.repeat(n) + '▱'.repeat(40 - n) + ' ' + p.toFixed(0).padStart(3, ' ') + '%';
}

function tickBars() {
  updateMini();
  if (state.mining && state.shareEtaStart > 0 && state.shareEtaTotal > 0) {
    const elapsed = (performance.now() - state.shareEtaStart) / 1000;
    const eta = state.shareEtaTotal;
    const pct = (elapsed / eta) * 100;
    $('shareBar').innerHTML = renderBar(pct, { blinkOnOverflow: true });
    if (pct > 100) {
      $('shareBarMeta').textContent =
        `${Math.floor(elapsed)}s elapsed · ~${Math.ceil(eta)}s mean · ${Math.ceil(elapsed - eta)}s over`;
    } else {
      $('shareBarMeta').textContent =
        `${Math.floor(elapsed)}s elapsed · ~${Math.ceil(eta)}s mean · ${Math.ceil(eta - elapsed)}s to go`;
    }
  } else {
    // Static idle render — bar at 0%, meta says why. No animation.
    $('shareBar').innerHTML = renderBar(0);
    $('shareBarMeta').textContent = state.mining ? 'awaiting hashrate' : 'not mining';
  }
}

function log(msg) {
  const el = $('log');
  if (!el) return;
  // Only auto-scroll to the bottom if the user is already there (within a few
  // px of slop for sub-pixel rounding). If they've scrolled up to read older
  // entries, leave their position alone.
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 4;
  const line = document.createElement('div');
  line.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
  el.appendChild(line);
  // Limite la taille du journal (le nœud 0 est la bannière, on le garde).
  while (el.childNodes.length > LOG_MAX_LINES) el.removeChild(el.childNodes[1]);
  if (atBottom) el.scrollTop = el.scrollHeight;
}

// ───── Écran allumé (évite la mise en veille pendant le minage) ──────
let wakeLock = null;
async function acquireWakeLock() {
  try {
    if ('wakeLock' in navigator && !wakeLock) {
      wakeLock = await navigator.wakeLock.request('screen');
      wakeLock.addEventListener('release', () => { wakeLock = null; });
    }
  } catch (_) { wakeLock = null; }
}
function releaseWakeLock() {
  try { if (wakeLock) wakeLock.release(); } catch (_) {}
  wakeLock = null;
}
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible' && state.mining) acquireWakeLock();
});

// ───── Reconnexion avec backoff ──────────────────────────────────────
function scheduleReconnect(reason) {
  if (!state.mining || state.reconnectTimer) return;
  if (state.worker) state.worker.postMessage({ type: 'stop' });
  const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * Math.pow(2, state.reconnectAttempt));
  state.reconnectAttempt++;
  log(`${reason}; reconnecting in ${Math.round(delay / 1000)}s...`);
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    connectWS();
  }, delay);
}

// Retour du réseau : on reconnecte tout de suite sans attendre le backoff.
window.addEventListener('online', () => {
  if (!state.mining) return;
  if (state.ws && state.ws.readyState === WebSocket.OPEN) return;
  log('Network back online; reconnecting now');
  if (state.reconnectTimer) { clearTimeout(state.reconnectTimer); state.reconnectTimer = null; }
  state.reconnectAttempt = 0;
  connectWS();
});

// Watchdog : si le proxy ne dit plus rien (le pool envoie un job toutes les
// ~30 s), la connexion est considérée comme morte.
setInterval(() => {
  if (!state.mining || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  if (performance.now() - state.lastMsgAt > WS_SILENCE_MS) {
    log('No data from proxy for 2 min; connection considered dead');
    const dead = state.ws;
    state.ws = null;
    try { dead.close(); } catch (_) {}
    scheduleReconnect('Proxy silent');
  }
}, 10000);

function connectWS() {
  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
    return;
  }

  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  state.nicehash = false;
  let ws;
  try {
    ws = new WebSocket(`${proto}//${location.host}`);
  } catch (e) {
    log('WebSocket creation failed: ' + (e && e.message));
    scheduleReconnect('WebSocket creation failed');
    return;
  }
  state.ws = ws;
  state.lastMsgAt = performance.now();

  ws.onopen = () => {
    if (state.ws !== ws) return;
    state.lastMsgAt = performance.now();
    state.status = 'connected to proxy';
    log('Connected to proxy');
    updateUI();

    // Tell the proxy which pool + wallet this session wants BEFORE login,
    // so it can reconnect upstream if the user customised them.
    const target = readConnectionInputs();
    ws.send(JSON.stringify({
      method: 'set_target',
      params: target,
      id: 0,
    }));
    log(`Pool target → ${target.host}:${target.port}, wallet ${(target.wallet || '').slice(0, 12)}…`);

    ws.send(JSON.stringify({
      method: 'login',
      params: {},
      id: 1,
    }));
  };

  ws.onmessage = (e) => {
    // Ignore les messages d'un ancien socket (reconnexion en cours).
    if (state.ws !== ws) return;
    state.lastMsgAt = performance.now();

    let msg;
    try {
      msg = JSON.parse(e.data);
    } catch (_) {
      log('Bad message from proxy (ignored)');
      return;
    }

    if (msg.error) {
      if (msg.id === 2) {
        state.rejected++;
        log('Share rejected: ' + (typeof msg.error === 'string' ? msg.error : JSON.stringify(msg.error)));
        updateUI();
        return;
      }

      const errorText = typeof msg.error === 'string' ? msg.error : JSON.stringify(msg.error);
      log('Error: ' + errorText);
      state.status = 'error';
      updateUI();
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
      scheduleReconnect('Pool connection error');
      return;
    }

    // Login response with first job
    if (msg.result && msg.result.job) {
      state.nicehash = Array.isArray(msg.result.extensions) && msg.result.extensions.includes('nicehash');
      state.reconnectAttempt = 0;   // connexion réussie : le backoff repart de 1 s
      log('Logged in, received first job');
      handleJob(msg.result.job);
    }

    // New job notification
    if (msg.method === 'job') {
      log('New job received');
      handleJob(msg.params);
    }

    // Share response. Login responses can also contain status OK, so only
    // count submit responses as shares.
    if (msg.id === 2 && msg.result && msg.result.status === 'OK') {
      state.accepted++;
      workerFailures = 0;           // le minage fonctionne : on oublie les anciennes erreurs
      log('Share accepted! Total: ' + state.accepted);
      // Share-ETA bar resets only here (and on Start). Snapshot the eta with
      // the now-stable hashrate so the bar fills smoothly toward the next
      // share without jittering.
      state.shareEtaStart = performance.now();
      state.shareEtaTotal = state.hashrate > 0 && state.currentJobDiff > 0
        ? state.currentJobDiff / state.hashrate
        : 0;
      updateUI();
    }
  };

  ws.onclose = (e) => {
    if (state.ws !== ws) return;
    state.status = 'disconnected';
    log(`Disconnected from proxy${e && e.code ? ` (code ${e.code})` : ''}`);
    updateUI();
    scheduleReconnect('Disconnected');
  };

  ws.onerror = () => {
    if (state.ws !== ws) return;
    log('WebSocket error');
  };
}

function handleJob(job) {
  if (!state.mining) return;
  job = { ...job, nicehash: state.nicehash };
  const diff = targetToDiff(job.target);
  state.currentJobId = job.job_id;
  state.currentJobSeq++;
  state.currentJobDiff = diff;
  state.lastJob = job;
  const eta = state.hashrate > 0 && diff > 0 ? `, est share ${Math.ceil(diff / state.hashrate)}s` : '';
  log(`Job diff ${diff || 'unknown'}${job.height ? ` height ${job.height}` : ''}${eta}`);
  // Worker may be null mid-rebuild; the cached lastJob is replayed on 'ready'.
  if (!state.worker || !state.workerReady) return;
  // Don't flip status to 'Mining' until the dataset is built — dataset_progress
  // owns the label until then.
  if (state.datasetBuilt || !fullMemory) state.status = `mining with ${datasetThreads} thread${datasetThreads === 1 ? '' : 's'}`;
  updateUI();
  state.worker.postMessage({
    type: 'job',
    blob: job.blob,
    target: job.target,
    seed_hash: job.seed_hash,
    job_id: job.job_id,
    job_seq: state.currentJobSeq,
    nicehash: job.nicehash,
  });
}

// ───── Récupération automatique du worker ────────────────────────────
// En cas d'erreur du worker (mémoire, crash WASM...), on le recrée au lieu
// de laisser la page figée. Dès la 2e erreur en mode Full, repli en Light.
// Après WORKER_MAX_FAILURES échecs sans share accepté, arrêt propre.
let workerFailures = 0;
let recoverTimer = null;

function recoverWorker(reason) {
  if (!state.mining || recoverTimer) return;
  workerFailures++;
  if (workerFailures > WORKER_MAX_FAILURES) {
    log(`Too many worker failures (${reason}); stopping. Reload the page to retry.`);
    toggle();
    state.status = 'stopped (worker errors)';
    updateUI();
    return;
  }
  if (fullMemory && workerFailures >= 2) {
    fullMemory = false;
    log('Full mode keeps failing: falling back to light mode');
  }
  log(`Recovering worker (${reason}), attempt ${workerFailures}/${WORKER_MAX_FAILURES}...`);
  if (state.worker) {
    try { state.worker.terminate(); } catch (_) {}
  }
  state.worker = null;
  state.workerReady = false;
  state.hashrate = 0;
  state.datasetBuilt = false;
  state.status = 'restarting worker…';
  updateUI();
  recoverTimer = setTimeout(() => {
    recoverTimer = null;
    if (state.mining) initWorker();
  }, 3000);
}

// Worker-shaped facade over N single-thread randomx_st workers (no SAB). It
// fans init/job/stop out (init with a disjoint nonce slot each), sums the
// per-worker hashrates, passes shares and errors through, emits 'ready' once
// all workers are ready and 'nonce_exhausted' once every slot has run out on
// that job, and forwards the chatty per-worker messages
// (status/jit/profile) from worker 0 only. With fbFull = K, workers 0..K-1 are
// replicas and an RxFbFull.FbCoordinator runs the dataset build per seed
// (its progress as 'dataset_progress'); 'mode' reports the full/light split.
class NoSabPool {
  constructor(n, vTag, fbFull = 0) {
    this.onmessage = null;
    this.onerror = null;
    this.onmessageerror = null;
    this.workers = [];
    this.rates = new Array(n).fill(0);
    this.ready = new Set();
    this.exhausted = new Map(); // `${job_id}/${job_seq}` -> Set of exhausted slots
    this.modes = new Array(n).fill(null);
    this.modeSent = '';
    this.full = Math.min(fbFull, n);
    this.fb = null;
    if (this.full > 0 && typeof RxFbFull !== 'undefined') {
      this.fb = new RxFbFull.FbCoordinator({
        n,
        full: [...Array(this.full).keys()],
        send: (i, m, transfer) => this.workers[i].postMessage(m, transfer || []),
        progress: (done, total, etaSec) => this._emit({ type: 'dataset_progress', done, total, etaSec, threads: n }),
        done: (seed, replicas) => this._emit({
          type: 'status', message: `fb_full: ${replicas.length}/${this.full} replica(s) mining in full mode`,
        }),
      });
    } else {
      this.full = 0;
    }
    for (let i = 0; i < n; i++) {
      const w = new Worker(`worker.js?v=${vTag}&build=st`, { name: `rx-st-${i}` });
      w.onmessage = (e) => this._recv(i, e.data);
      w.onerror = (e) => { if (this.onerror) this.onerror(e); };
      w.onmessageerror = (e) => { if (this.onmessageerror) this.onmessageerror(e); };
      this.workers.push(w);
    }
  }

  _emit(data) {
    if (this.onmessage) this.onmessage({ data });
  }

  _recv(i, msg) {
    if (this.fb && this.fb.recv(i, msg)) return;
    switch (msg.type) {
      case 'ready':
        this.ready.add(i);
        if (this.ready.size === this.workers.length) this._emit(msg);
        break;
      case 'hashrate':
        this.rates[i] = msg.rate;
        this._emit({ type: 'hashrate', rate: this.rates.reduce((a, b) => a + b, 0) });
        break;
      case 'mode': {
        this.modes[i] = msg.mode;
        const n = this.workers.length;
        const nFull = this.modes.filter((m) => m === 'full').length;
        const mode = nFull ? `${nFull} full + ${n - nFull} light (no SAB)` : `light (no SAB, ${n} workers)`;
        if (mode !== this.modeSent) {
          this.modeSent = mode;
          this._emit({ ...msg, mode });
        }
        break;
      }
      case 'nonce_exhausted': {
        // A worker exhausts only its own slot: its rate leaves the sum now,
        // the page is out of nonces once every slot is, for the same job.
        this.rates[i] = 0;
        this._emit({ type: 'hashrate', rate: this.rates.reduce((a, b) => a + b, 0) });
        const key = `${msg.job_id}/${msg.job_seq}`;
        const slots = this.exhausted.get(key) || new Set();
        this.exhausted.set(key, slots.add(i));
        if (slots.size === this.workers.length) {
          this.exhausted.delete(key);
          this._emit(msg);
        }
        break;
      }
      case 'share':
        this._emit(msg);
        break;
      case 'error':
        this._emit({ ...msg, message: `[worker ${i}] ${msg.message}` });
        break;
      default:
        if (i === 0) this._emit(msg);
    }
  }

  postMessage(msg) {
    if (msg.type === 'stop') this.rates.fill(0);
    if (msg.type === 'stop' || msg.type === 'job') this.exhausted.clear();
    // a new seed starts a build (the workers report fb_cache once rekeyed)
    if (this.fb && msg.type === 'job') this.fb.epoch(msg.seed_hash);
    const n = this.workers.length;
    this.workers.forEach((w, i) => {
      w.postMessage(msg.type === 'init'
        ? { ...msg, fullMemory: false, datasetThreads: 1, datasetInitThreads: 1, nonceSlot: i, nonceSlots: n,
            ...(this.fb ? { fbRole: i < this.full ? 'full' : 'light' } : {}) }
        : msg);
    });
  }

  terminate() {
    for (const w of this.workers) {
      try { w.terminate(); } catch (_) {}
    }
    this.workers = [];
  }
}

function initWorker() {
  if (state.worker) {
    if (state.workerReady) {
      connectWS();
    } else {
      state.pendingConnect = true;
    }
    return;
  }
  // ?coi=1: index.html is registering coi-sw.js and about to reload into an
  // isolated page; start after that (or on the no-SAB path if it gives up).
  if (window.__coiPending) {
    if (!state.coiWait) {
      state.coiWait = true;
      log('coi: waiting for the service worker reload...');
      window.addEventListener('coi-settled', () => { if (state.mining) initWorker(); }, { once: true });
    }
    return;
  }

  const vTag = (window.MINER_BUILD || 'dev').replace(/[^a-zA-Z0-9-]/g, '');
  try {
    state.worker = noSab ? new NoSabPool(datasetThreads, vTag, fbFull) : new Worker(`worker.js?v=${vTag}`);
  } catch (e) {
    state.worker = null;
    log('Worker creation failed: ' + (e && e.message));
    recoverWorker('worker creation failed');
    return;
  }
  log(`build=${window.MINER_BUILD || '?'} (cside-jit split)`);
  if (isMobile) {
    log(`Mobile détecté : max ${MAX_THREADS} threads, mode Full désactivé`);
  }
  if (noSab) {
    log(`No SharedArrayBuffer${params.get('sab') === '0' ? ' (forced by ?sab=0)' : ''}: ` +
        `${datasetThreads} single-thread light-mode workers (randomx_st, ~300 MB each)`);
    if (fbFull) {
      log(state.worker.full
        ? `fb_full=${fbFull}: ${state.worker.full} of them build and mine on a full dataset replica (~2.3 GB each)`
        : `fb_full=${fbFull}: fb_full.js not loaded, staying in light mode`);
    }
  }
  if (!enableJit) {
    log('JIT disabled by URL param.');
  }
  log(`crossOriginIsolated=${window.crossOriginIsolated === true}`);
  const swCtl = navigator.serviceWorker && navigator.serviceWorker.controller;
  if (swCtl && /\/coi-sw\.js$/.test(swCtl.scriptURL)) log('coi: service worker active (COOP/COEP injected; ?coi=0 removes it)');
  if (window.__coiLog) log(window.__coiLog);
  log(`navigator.hardwareConcurrency=${navigator.hardwareConcurrency || 'unknown'}, mining threads=${datasetThreads}, init threads=${datasetInitThreads}`);

  const thisWorker = state.worker;

  state.worker.onerror = (e) => {
    if (state.worker !== thisWorker) return;
    const where = e.filename ? ` (${e.filename}:${e.lineno || 0})` : '';
    log(`Worker error: ${e.message || 'unknown error'}${where}`);
    state.status = 'worker error';
    updateUI();
    recoverWorker('worker error');
  };

  state.worker.onmessageerror = () => {
    if (state.worker !== thisWorker) return;
    log('Worker message error');
    state.status = 'worker error';
    updateUI();
  };

  state.worker.onmessage = (e) => {
    if (state.worker !== thisWorker) return;
    const msg = e.data;
    switch (msg.type) {
      case 'ready':
        log('WASM module loaded');
        state.workerReady = true;
        state.status = 'wasm ready';
        updateUI();
        if (state.mining || state.pendingConnect) {
          state.pendingConnect = false;
          connectWS();
        }
        // Replay the cached job after a rebuild so the fresh worker
        // rebuilds the dataset from the current seed and resumes hashing.
        if (state.mining && state.lastJob) {
          log('Replaying last job into fresh worker');
          const job = state.lastJob;
          state.worker.postMessage({
            type: 'job',
            blob: job.blob,
            target: job.target,
            seed_hash: job.seed_hash,
            job_id: job.job_id,
            job_seq: state.currentJobSeq,
            nicehash: job.nicehash,
          });
        }
        break;
      case 'hashrate':
        // Drop stale rate reports that arrive after a Stop — the worker's
        // last mineLoop slice posts one more rate before it sees mining=false,
        // and without this guard the cell would be pinned to a non-zero value
        // even though we're idle.
        if (!state.mining) break;
        state.hashrate = msg.rate;
        if (state.hashrate > state.hashrateMax) state.hashrateMax = state.hashrate;
        // Share-ETA: anchor the start clock on the first non-zero hashrate,
        // and TRACK the minimum eta seen since then (max hashrate). The first
        // sample is the worker's partial sub-second post-dataset slice and is
        // wildly pessimistic; locking it would peg the bar at ~0% for ages.
        // Tracking the minimum gives an optimistic eta that drops as the
        // worker ramps up, while keeping pct monotonically increasing.
        if (state.hashrate > 0 && state.currentJobDiff > 0) {
          const candidate = state.currentJobDiff / state.hashrate;
          if (state.shareEtaStart === 0) state.shareEtaStart = performance.now();
          if (state.shareEtaTotal === 0 || candidate < state.shareEtaTotal) {
            state.shareEtaTotal = candidate;
          }
        }
        updateUI();
        break;
      case 'share':
        log(`Found share! Nonce: ${msg.nonce}, share diff ${msg.share_diff || 'unknown'} / job diff ${msg.target_diff || state.currentJobDiff || 'unknown'}`);
        if (msg.job_id !== state.currentJobId) {
          log(`Dropped stale share for old job ${msg.job_id}`);
          break;
        }
        if (msg.job_seq && msg.job_seq !== state.currentJobSeq) {
          log(`Dropped stale share for old job sequence ${msg.job_seq}`);
          break;
        }
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
          state.ws.send(JSON.stringify({
            method: 'submit',
            params: {
              job_id: msg.job_id,
              nonce: msg.nonce,
              result: msg.result,
            },
            id: 2,
          }));
        } else {
          log('Share not submitted: pool connection is not open');
        }
        break;
      case 'nonce_exhausted':
        if (state.mining && msg.job_id === state.currentJobId && msg.job_seq === state.currentJobSeq) {
          state.hashrate = 0;
          state.status = 'nonce range exhausted; waiting for a new job';
          log(state.status);
          updateUI();
        }
        break;
      case 'mode':
        state.mode = msg.mode;
        log(`Mining mode: ${msg.mode}`);
        // 'mode' fires after the dataset is fully built (or immediately for
        // light mode). Safe to consider the dataset ready from this point.
        state.datasetBuilt = true;
        updateUI();
        break;
      case 'jit': {
        const stats = msg.stats || {};
        if (stats.enabled && stats.failCount && stats.lastError && stats.lastError !== state.lastJitError) {
          state.lastJitError = stats.lastError;
          log(`JIT fallback: ${stats.lastError}`);
        }
        break;
      }
      case 'profile': {
        const total = msg.initMs + msg.runMs + msg.finalMs;
        if (total > 0) {
          const pct = (v) => ((v / total) * 100).toFixed(1);
          const runOtherMs = Math.max(0, msg.runMs - (msg.bytecodeMs || 0));
          log(`Core profile ${Math.round(msg.hashes)} hashes: init ${pct(msg.initMs)}%, bytecode ${pct(msg.bytecodeMs || 0)}%, run-other ${pct(runOtherMs)}%, final ${pct(msg.finalMs)}%`);
        }
        break;
      }
      case 'status':
        // Don't overwrite state.status with chatty worker text — that was the
        // cause of the flashing/overflow in the Status field. Log-only.
        log(msg.message);
        break;
      case 'dataset_progress': {
        const pct = msg.total > 0 ? (msg.done / msg.total) * 100 : 0;
        $('datasetBarRow').hidden = false;
        $('datasetBar').textContent = renderBar(pct);
        $('datasetBarMeta').textContent =
          msg.done >= msg.total
            ? `complete · ${msg.threads} init threads`
            : `~${msg.etaSec}s remaining · ${msg.threads} init threads`;
        if (msg.done >= msg.total) {
          state.datasetBuilt = true;
          if (state.mining) state.status = `mining with ${datasetThreads} thread${datasetThreads === 1 ? '' : 's'}`;
        } else {
          state.datasetBuilt = false;
          state.status = 'building dataset';
        }
        updateUI();
        break;
      }
      case 'error':
        log('Error: ' + msg.message);
        state.status = 'error';
        updateUI();
        recoverWorker('worker reported an error');
        break;
    }
  };

  const jitExperiment = params.get('jit_exp') || '';
  if (jitExperiment) {
    const tokens = jitExperiment.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
    const wrongHash = tokens.includes('reuse') || tokens.includes('reuse2');
    if (wrongHash) {
      log(`JIT experiment mode active: ${jitExperiment} (hashes will be WRONG; use only for timing analysis)`);
    } else {
      log(`JIT experiment mode active: ${jitExperiment} (hashes are correct)`);
    }
  }
  const jitProfile = params.get('jit_profile') || 'auto';
  state.worker.postMessage({ type: 'init', enableJit, fullMemory, datasetThreads, datasetInitThreads, profileCore, jitExperiment, jitProfile });
}

// Reads the threads input (the only source of truth the user can edit) and
// returns a clamped 1-MAX_THREADS integer.
function readDesiredThreads() {
  const inputEl = $('rebuildThreads');
  if (!inputEl) return datasetThreads;
  return Math.max(1, Math.min(MAX_THREADS, Math.floor(Number(inputEl.value) || 1)));
}

function toggle() {
  $('toggle').classList.remove('pulse');
  if ($('miniToggle')) $('miniToggle').classList.remove('pulse');
  if (state.mining) {
    state.mining = false;
    state.hashrate = 0;
    state.hashrateMax = 0;
    state.status = 'stopped';
    state.currentJobId = null;
    state.currentJobSeq = 0;
    state.currentJobDiff = 0;
    state.shareEtaStart = 0;
    state.shareEtaTotal = 0;
    state.datasetBuilt = false;
    state.lastJob = null;
    state.reconnectAttempt = 0;
    workerFailures = 0;
    $('datasetBar').textContent = renderBar(0);
    $('datasetBarMeta').textContent = 'not started';
    state.mode = 'standby';
    if (state.worker) state.worker.postMessage({ type: 'stop' });
    if (state.ws) state.ws.close();
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
    if (recoverTimer) {
      clearTimeout(recoverTimer);
      recoverTimer = null;
    }
    state.ws = null;
    releaseWakeLock();
    log('Mining stopped');
  } else {
    const desired = readDesiredThreads();
    if (desired !== datasetThreads) {
      datasetThreads = desired;
      if (state.worker) {
        log(`Threads changed to ${datasetThreads}; terminating stale worker`);
        try { state.worker.terminate(); } catch (_) {}
        state.worker = null;
        state.workerReady = false;
      }
    }
    state.mining = true;
    state.status = 'starting…';
    state.shareEtaStart = 0;
    state.shareEtaTotal = 0;
    state.hashrateMax = 0;
    state.datasetBuilt = false;
    state.reconnectAttempt = 0;
    workerFailures = 0;
    acquireWakeLock();
    log(`Starting miner${enableJit ? '' : ' without JIT'} in ${fullMemory ? `full mode (${datasetThreads} threads)` : 'light mode'}${profileCore ? ' with core profile' : ''}...`);
    initWorker();
  }
  updateUI();
}

function hardRebuild(newThreads) {
  const clamped = Math.max(1, Math.min(MAX_THREADS, Math.floor(Number(newThreads) || 1)));
  datasetThreads = clamped;

  if (state.worker) {
    try { state.worker.terminate(); } catch (_) {}
    state.worker = null;
    state.workerReady = false;
  }

  if (!state.mining) {
    log(`Hard rebuild: starting miner with ${datasetThreads} threads`);
    toggle();
    return;
  }
  if (!state.lastJob) {
    log('Hard rebuild: no job cached yet, will pick up first job from pool');
    initWorker();
    return;
  }

  log(`Hard rebuild: respawning worker with ${datasetThreads} threads`);
  state.status = `rebuilding with ${datasetThreads} thread${datasetThreads === 1 ? '' : 's'}…`;
  state.hashrate = 0;
  state.hashrateMax = 0;
  state.shareEtaStart = 0;
  state.shareEtaTotal = 0;
  state.datasetBuilt = false;
  $('datasetBarRow').hidden = false;
  $('datasetBar').textContent = renderBar(0);
  $('datasetBarMeta').textContent = 'starting…';
  updateUI();
  initWorker();
}

$('toggle').addEventListener('click', toggle);

const threadsInput = $('rebuildThreads');
const miniThreadsInput = $('miniThreads');
// Le plafond de threads s'applique aussi aux champs de saisie.
if (threadsInput) threadsInput.max = String(MAX_THREADS);
if (miniThreadsInput) miniThreadsInput.max = String(MAX_THREADS);
if (threadsInput) {
  threadsInput.value = String(datasetThreads);
  $('rebuildBtn').addEventListener('click', () => {
    hardRebuild(threadsInput.value);
  });
}
if (miniThreadsInput) {
  miniThreadsInput.value = String(datasetThreads);
  miniThreadsInput.addEventListener('input', () => {
    if (threadsInput) threadsInput.value = miniThreadsInput.value;
  });
  if (threadsInput) {
    threadsInput.addEventListener('input', () => {
      miniThreadsInput.value = threadsInput.value;
    });
  }
  $('miniToggle').addEventListener('click', toggle);
  $('miniRebuild').addEventListener('click', () => {
    hardRebuild(miniThreadsInput.value);
  });
}

// Rafraîchit la barre « Next share ETA » et le mini-affichage.
setInterval(tickBars, 500);

// ───── Connection settings (wallet + pool host:port + presets) ────────

const LS_WALLET    = 'rxminer.wallet';
const LS_POOL_HOST = 'rxminer.pool_host';
const LS_POOL_PORT = 'rxminer.pool_port';
const LS_WORKER    = 'rxminer.worker';

const POOL_PRESETS = {
  supportxmr:   { host: 'pool.supportxmr.com',     port: 3333  },
  moneroocean:  { host: 'gulf.moneroocean.stream', port: 10004 },
};

const MINER_DEFAULTS = (window.MINER_DEFAULTS && typeof window.MINER_DEFAULTS === 'object')
  ? window.MINER_DEFAULTS
  : { wallet: '', poolHost: '', poolPort: '' };

function readStored(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    return (v == null || v === '') ? fallback : v;
  } catch (_) { return fallback; }
}

const walletInput   = $('walletInput');
const poolHostInput = $('poolHostInput');
const poolPortInput = $('poolPortInput');
const workerInput   = $('workerInput');

function readConnectionInputs() {
  const wallet = walletInput ? (walletInput.dataset.full || walletInput.value.trim()) : (MINER_DEFAULTS.wallet || '');
  const host = poolHostInput ? (poolHostInput.value.trim() || MINER_DEFAULTS.poolHost) : (MINER_DEFAULTS.poolHost || '');
  const port = poolPortInput ? (parseInt(poolPortInput.value, 10) || Number(MINER_DEFAULTS.poolPort) || 3333) : 3333;
  const worker = workerInput ? workerInput.value.trim() : '';
  return { wallet, host, port, worker };
}

if (walletInput) {
  const savedWallet = readStored(LS_WALLET, MINER_DEFAULTS.wallet || '');
  walletInput.value = savedWallet;
  walletInput.dataset.full = savedWallet;

  walletInput.addEventListener('focus', () => {
    walletInput.value = walletInput.dataset.full || walletInput.value;
  });

  walletInput.addEventListener('blur', () => {
    const full = walletInput.value.trim();
    walletInput.dataset.full = full;
    try { localStorage.setItem(LS_WALLET, full); } catch (_) {}
    if (full.length > 16) {
      walletInput.value = full.slice(0, 8) + '……' + full.slice(-8);
    }
  });

  if (savedWallet.length > 16) {
    walletInput.value = savedWallet.slice(0, 8) + '……' + savedWallet.slice(-8);
  }
}

if (poolHostInput) {
  poolHostInput.value = readStored(LS_POOL_HOST, MINER_DEFAULTS.poolHost || 'gulf.moneroocean.stream');
  poolHostInput.addEventListener('input', () => {
    try { localStorage.setItem(LS_POOL_HOST, poolHostInput.value.trim()); } catch (_) {}
  });
}

if (poolPortInput) {
  poolPortInput.value = readStored(LS_POOL_PORT, String(MINER_DEFAULTS.poolPort || 10004));
  poolPortInput.addEventListener('input', () => {
    try { localStorage.setItem(LS_POOL_PORT, poolPortInput.value.trim()); } catch (_) {}
  });
}

if (workerInput) {
  workerInput.value = readStored(LS_WORKER, '');
  workerInput.addEventListener('input', () => {
    try { localStorage.setItem(LS_WORKER, workerInput.value.trim()); } catch (_) {}
  });
}

function applyPreset(key) {
  const preset = POOL_PRESETS[key];
  if (preset && poolHostInput && poolPortInput) {
    poolHostInput.value = preset.host;
    poolPortInput.value = preset.port;
    try {
      localStorage.setItem(LS_POOL_HOST, preset.host);
      localStorage.setItem(LS_POOL_PORT, preset.port);
    } catch (_) {}
    log(`Applied preset: ${key} (${preset.host}:${preset.port})`);
  }
}

document.querySelectorAll('[data-preset]').forEach((btn) => {
  btn.addEventListener('click', () => applyPreset(btn.getAttribute('data-preset')));
});

// index.html n'a pas d'attribut data-preset : on lie aussi les boutons par id.
[['presetSupportxmr', 'supportxmr'], ['presetMoneroocean', 'moneroocean']].forEach(([id, key]) => {
  const btn = $(id);
  if (btn && !btn.hasAttribute('data-preset')) {
    btn.addEventListener('click', () => applyPreset(key));
  }
});
