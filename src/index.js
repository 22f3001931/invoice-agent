// A2A 1.0 HTTP+JSON invoice action agent for Cloudflare Workers.
// Layers: (1) protocol router  (2) Principal Durable Object = storage + lifecycle
//         (3) decision layer = cache + one batched model call.

const MT_BATCH = 'application/vnd.ga5.invoice-claim-batch+json';
const MT_PROPOSALS = 'application/vnd.ga5.invoice-action-proposals+json';
const MT_RESULTS = 'application/vnd.ga5.invoice-action-results+json';
const MT_RECEIPTS = 'application/vnd.ga5.invoice-action-receipts+json';
const A2A = 'application/a2a+json';
const ACTIONS = ['settle_invoice', 'request_approval', 'hold_invoice', 'reject_duplicate', 'open_exception'];
const TERMINAL = new Set(['TASK_STATE_COMPLETED', 'TASK_STATE_CANCELED', 'TASK_STATE_FAILED', 'TASK_STATE_REJECTED']);

/* ---------- helpers ---------- */
const enc = new TextEncoder();
async function sha256(s) {
  const b = await crypto.subtle.digest('SHA-256', enc.encode(s));
  return [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, '0')).join('');
}
function canon(v) {
  if (Array.isArray(v)) return '[' + v.map(canon).join(',') + ']';
  if (v && typeof v === 'object')
    return '{' + Object.keys(v).sort().map((k) => JSON.stringify(k) + ':' + canon(v[k])).join(',') + '}';
  return JSON.stringify(v);
}
const ts = () => new Date().toISOString();
function respond(status, body, type = A2A, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': type, 'cache-control': 'no-store', ...extra },
  });
}
// Generic error bodies: never echo ids or reveal whether a resource exists.
const err = (status, code, message, extra) =>
  respond(status, { error: { code, status, message } }, A2A, extra);

/* ---------- Agent Card ---------- */
function card(origin, base) {
  const u = origin + base;
  const inputs = [MT_BATCH, MT_RESULTS];
  const outputs = [MT_PROPOSALS, MT_RECEIPTS];
  return respond(
    200,
    {
      name: 'Invoice Action Agent',
      description:
        'Reads invoice packages, proposes one evidence-backed business action per invoice, and records receipts only for accepted actions.',
      version: '1.0.0',
      capabilities: { streaming: false, pushNotifications: false },
      supportedInterfaces: [
        { url: u, protocolBinding: 'HTTP+JSON', protocolVersion: '1.0' },
        { url: u + '/', protocolBinding: 'HTTP+JSON', protocolVersion: '1.0' },
      ],
      securitySchemes: { bearer: { httpAuthSecurityScheme: { scheme: 'Bearer' } } },
      securityRequirements: [{ schemes: { bearer: { list: [] } } }],
      defaultInputModes: inputs,
      defaultOutputModes: outputs,
      skills: [
        {
          id: 'invoice_action_agent',
          name: 'Invoice action agent',
          description:
            'Chooses settle, approve, hold, reject-duplicate or exception actions for invoice packages and cites the decisive evidence.',
          tags: ['invoice', 'accounts-payable', 'approval', 'duplicate-detection'],
          inputModes: inputs,
          outputModes: outputs,
        },
      ],
    },
    'application/json'
  );
}

/* ---------- Worker entry: auth, version, media type, route to principal DO ---------- */
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';
    const base = (env.BASE_PATH || '/a2a').replace(/\/+$/, '');

    if (path === '/.well-known/agent-card.json' || path === base + '/.well-known/agent-card.json') {
      if (req.method !== 'GET' && req.method !== 'HEAD') return err(405, 'METHOD_NOT_ALLOWED', 'Method not allowed.');
      return card(url.origin, base);
    }
    if (!(path === base || path.startsWith(base + '/'))) return err(404, 'NOT_FOUND', 'Not found.');

    const m = /^Bearer\s+(\S+)$/i.exec(req.headers.get('authorization') || '');
    if (!m) return err(401, 'UNAUTHENTICATED', 'Authentication required.', { 'www-authenticate': 'Bearer' });
    if (req.headers.get('a2a-version') !== '1.0')
      return err(400, 'VERSION_NOT_SUPPORTED', 'Only A2A-Version 1.0 is supported.');

    let body;
    if (req.method === 'POST') {
      const ct = (req.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
      if (ct !== A2A && ct !== 'application/json')
        return err(415, 'UNSUPPORTED_MEDIA_TYPE', 'Use application/a2a+json.');
      body = await req.text();
    }
    const rest = path.slice(base.length) || '/';
    // Each Bearer token is its own principal -> its own Durable Object.
    const stub = env.PRINCIPAL.get(env.PRINCIPAL.idFromName(await sha256(m[1])));
    return stub.fetch(new Request('https://principal.internal' + rest, { method: req.method, body }));
  },
};

/* ---------- Principal Durable Object: all task state, serialized & atomic ---------- */
export class Principal {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.tasks = new Map(); // taskId -> {task, meta:{batchId, proposals}}
    this.msgs = new Map(); // messageId -> {hash, taskId, promise?}
    state.blockConcurrencyWhile(async () => {
      const all = await state.storage.list();
      for (const [k, v] of all) {
        if (k.startsWith('task:')) this.tasks.set(k.slice(5), v);
        else if (k.startsWith('msg:')) this.msgs.set(k.slice(4), v);
      }
    });
  }

  persist(rec, msgId) {
    const o = { ['task:' + rec.task.id]: rec };
    if (msgId) {
      const e = this.msgs.get(msgId);
      o['msg:' + msgId] = { hash: e.hash, taskId: e.taskId };
    }
    return this.state.storage.put(o);
  }

  async fetch(req) {
    const p = new URL(req.url).pathname.replace(/\/+$/, '') || '/';
    try {
      if (req.method === 'POST' && p === '/message:send') return await this.send(await req.text());
      if (req.method === 'GET' && p === '/tasks') return this.list();
      let m;
      if (req.method === 'GET' && (m = /^\/tasks\/([^/:]+)$/.exec(p))) return this.get(decodeURIComponent(m[1]));
      if (req.method === 'POST' && (m = /^\/tasks\/([^/]+):cancel$/.exec(p))) return await this.cancel(decodeURIComponent(m[1]));
      return err(404, 'NOT_FOUND', 'Not found.');
    } catch (e) {
      console.error('internal', e && e.message);
      return err(500, 'INTERNAL', 'Internal error.');
    }
  }

  get(id) {
    const rec = this.tasks.get(id);
    return rec ? respond(200, rec.task) : err(404, 'NOT_FOUND', 'Task not found.');
  }

  list() {
    let tasks = [...this.tasks.values()].map((r) => r.task).sort((a, b) => b.status.timestamp.localeCompare(a.status.timestamp));
    let s = JSON.stringify({ tasks });
    if (s.length > 400000) tasks = tasks.map(({ history, ...t }) => t); // keep under 512 KiB
    while (tasks.length > 1 && JSON.stringify({ tasks }).length > 400000) tasks.pop();
    return respond(200, { tasks });
  }

  async cancel(id) {
    const rec = this.tasks.get(id);
    if (!rec) return err(404, 'NOT_FOUND', 'Task not found.');
    // Synchronous check-and-set: first of cancel/result wins, the other sees a terminal task.
    if (TERMINAL.has(rec.task.status.state)) return err(409, 'TASK_TERMINAL', 'Task is already finished.');
    rec.task.status = { state: 'TASK_STATE_CANCELED', timestamp: ts() };
    await this.persist(rec);
    return respond(200, rec.task);
  }

  async send(text) {
    let body;
    try { body = JSON.parse(text); } catch { return err(400, 'INVALID_ARGUMENT', 'Malformed JSON.'); }
    const msg = body && body.message;
    if (!msg || typeof msg !== 'object' || typeof msg.messageId !== 'string' || !msg.messageId ||
        msg.role !== 'ROLE_USER' || !Array.isArray(msg.parts) || !msg.parts.length)
      return err(400, 'INVALID_ARGUMENT', 'Invalid message.');

    const hash = await sha256(canon(msg)); // message only; configuration ignored
    // ---- everything from here to registration is synchronous (no awaits) ----
    const prior = this.msgs.get(msg.messageId);
    if (prior) {
      if (prior.hash !== hash) return err(409, 'IDEMPOTENCY_CONFLICT', 'Message ID was reused with different content.');
      if (prior.promise) await prior.promise; // concurrent equivalent request: wait, no 2nd model call
      const rec = this.tasks.get(prior.taskId);
      return rec ? respond(200, { task: rec.task }) : err(404, 'NOT_FOUND', 'Task not found.');
    }
    if (msg.taskId) return this.continueTask(msg, hash);

    const bp = msg.parts.find((p) => p && p.mediaType === MT_BATCH);
    const data = bp && bp.data;
    if (!data || typeof data.batchId !== 'string' || !Array.isArray(data.packages) || !data.packages.length)
      return err(400, 'INVALID_ARGUMENT', 'Invalid invoice batch.');

    const taskId = crypto.randomUUID();
    const contextId = typeof msg.contextId === 'string' && msg.contextId ? msg.contextId : crypto.randomUUID();
    const rec = {
      task: { id: taskId, contextId, status: { state: 'TASK_STATE_WORKING', timestamp: ts() }, history: [msg], artifacts: [] },
      meta: { batchId: data.batchId, proposals: null },
    };
    this.tasks.set(taskId, rec);
    const entry = { hash, taskId };
    this.msgs.set(msg.messageId, entry);
    entry.promise = this.run(rec, data).finally(() => { delete entry.promise; });
    // ---- end synchronous section ----
    await this.persist(rec, msg.messageId); // state stored before responding
    await entry.promise;
    return respond(200, { task: rec.task });
  }

  async run(rec, data) {
    try {
      const decisions = await decidePackages(this.env, data.packages);
      const seen = new Set();
      const proposals = await Promise.all(
        data.packages.map(async (pkg, i) => {
          const d = decisions[i];
          let packageId = String(pkg.packageId ?? pkg.package_id ?? pkg.id ?? 'pkg-' + i);
          if (seen.has(packageId)) packageId += '-' + i;
          seen.add(packageId);
          const actionId = 'act_' + (await sha256(`${data.batchId}|${packageId}|${d.action}`)).slice(0, 32);
          return {
            packageId, actionId, action: d.action, facts: d.facts, evidenceRefs: d.evidenceRefs,
            rationale: buildRationale(d),
          };
        })
      );
      if (TERMINAL.has(rec.task.status.state)) return; // canceled while working
      rec.meta.proposals = proposals;
      rec.task.artifacts = [{
        artifactId: 'proposals-' + rec.task.id, name: 'invoice-action-proposals',
        parts: [{ mediaType: MT_PROPOSALS, data: { batchId: data.batchId, proposals } }],
      }];
      rec.task.status = { state: 'TASK_STATE_INPUT_REQUIRED', timestamp: ts() };
      await this.persist(rec);
    } catch (e) {
      console.error('run failed', e && e.message);
      if (!TERMINAL.has(rec.task.status.state)) {
        rec.task.status = { state: 'TASK_STATE_FAILED', timestamp: ts() };
        await this.persist(rec);
      }
    }
  }

  // Synchronous validation + state change (only the final persist is awaited).
  async continueTask(msg, hash) {
    const bad = () => err(400, 'INVALID_CONTINUATION', 'Continuation does not match the task.');
    const rec = this.tasks.get(msg.taskId);
    if (!rec) return err(404, 'NOT_FOUND', 'Task not found.');
    if (msg.contextId !== rec.task.contextId) return bad();
    const st = rec.task.status.state;
    if (TERMINAL.has(st)) return err(409, 'TASK_TERMINAL', 'Task is already finished.');
    if (st !== 'TASK_STATE_INPUT_REQUIRED' || !rec.meta.proposals) return err(409, 'TASK_NOT_READY', 'Task is not awaiting results.');

    const part = msg.parts.length === 1 ? msg.parts[0] : null;
    const data = part && part.mediaType === MT_RESULTS ? part.data : null;
    if (!data || data.batchId !== rec.meta.batchId || !Array.isArray(data.results) || !data.results.length) return bad();

    const byPkg = new Map(rec.meta.proposals.map((p) => [p.packageId, p]));
    const seen = new Set();
    const executions = [];
    for (const r of data.results) {
      if (!r || typeof r !== 'object') return bad();
      const p = byPkg.get(r.packageId);
      if (!p || seen.has(r.packageId)) return bad();
      seen.add(r.packageId);
      if (r.actionId !== p.actionId || r.action !== p.action) return bad();
      if (r.outcome !== 'ACCEPTED' && r.outcome !== 'REJECTED') return bad();
      if (r.outcome === 'ACCEPTED') {
        if (typeof r.receiptNonce !== 'string' || !r.receiptNonce) return bad();
        executions.push({
          packageId: p.packageId, actionId: p.actionId, action: p.action, receiptNonce: r.receiptNonce,
          facts: p.facts, evidenceRefs: p.evidenceRefs,
        });
      }
    }
    rec.task.history.push(msg);
    rec.task.artifacts.push({
      artifactId: 'receipts-' + rec.task.id, name: 'invoice-action-receipts',
      parts: [{ mediaType: MT_RECEIPTS, data: { batchId: rec.meta.batchId, executions } }],
    });
    rec.task.status = { state: 'TASK_STATE_COMPLETED', timestamp: ts() };
    this.msgs.set(msg.messageId, { hash, taskId: rec.task.id });
    await this.persist(rec, msg.messageId);
    return respond(200, { task: rec.task });
  }
}

function buildRationale(d) {
  const refs = d.evidenceRefs.join(', ');
  let s = `Chose ${d.action} for invoice ${d.facts.invoiceNumber} from ${d.facts.vendorName}. ${d.reason || ''} Decisive evidence: ${refs}.`;
  if (s.length < 60) s += ' The cited references in the decisive paragraph support this action.';
  return s.slice(0, 1500);
}

/* ---------- Decision cache: global Durable Object keyed by canonical package content ---------- */
export class DecisionCache {
  constructor(state) { this.s = state.storage; }
  async fetch(req) {
    const path = new URL(req.url).pathname;
    const body = await req.json();
    if (path === '/get') {
      const out = {};
      for (let i = 0; i < body.length; i += 100) {
        const m = await this.s.get(body.slice(i, i + 100).map((k) => 'd:' + k));
        for (const [k, v] of m) out[k.slice(2)] = v;
      }
      return Response.json(out);
    }
    const entries = Object.entries(body).map(([k, v]) => ['d:' + k, v]);
    for (let i = 0; i < entries.length; i += 100) await this.s.put(Object.fromEntries(entries.slice(i, i + 100)));
    return Response.json({ ok: true });
  }
}

/* ---------- Decision layer ---------- */
function render(v, ind = '') {
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map((x) => ind + '- ' + render(x, ind + '  ')).join('\n');
  if (v && typeof v === 'object')
    return Object.entries(v)
      .map(([k, x]) => `${ind}${k}: ${x && typeof x === 'object' ? '\n' + render(x, ind + '  ') : render(x, ind)}`)
      .join('\n');
  return String(v);
}
const stripIds = ({ packageId, package_id, id, ...rest }) => rest;
const bracketRefs = (text) => [...new Set(text.match(/\[[^\[\]\n]{1,80}\]/g) || [])];

async function decidePackages(env, packages) {
  const keys = await Promise.all(packages.map((p) => sha256(canon(stripIds(p)))));
  const cache = env.CACHE.get(env.CACHE.idFromName('global'));
  const hits = await (await cache.fetch('https://cache.internal/get', { method: 'POST', body: JSON.stringify(keys) })).json();
  const out = new Array(packages.length);
  const todo = [];
  keys.forEach((k, i) => (hits[k] ? (out[i] = hits[k]) : todo.push(i)));
  if (todo.length) {
    const fresh = await modelDecide(env, todo.map((i) => packages[i]));
    const store = {};
    todo.forEach((i, j) => {
      if (fresh[j]) { out[i] = fresh[j]; store[keys[i]] = fresh[j]; }
      else out[i] = fallback(packages[i]); // never cached
    });
    if (Object.keys(store).length)
      await cache.fetch('https://cache.internal/put', { method: 'POST', body: JSON.stringify(store) });
  }
  return out;
}

function fallback(pkg) {
  return {
    action: 'open_exception',
    facts: { vendorName: 'unknown', invoiceNumber: 'unknown', amountMinor: 0, currency: 'INR' },
    evidenceRefs: bracketRefs(render(pkg)).slice(0, 3),
    reason: 'Automated extraction failed, so this package is escalated for manual review.',
  };
}

function validate(d, pkg) {
  if (!d || !ACTIONS.includes(d.action)) return null;
  const text = render(pkg);
  const refs = [...new Set((d.evidenceRefs || []).map(String))].filter((r) => /^\[[^\[\]]+\]$/.test(r) && text.includes(r)).slice(0, 3);
  if (refs.length < 2) return null;
  const amount = Number(d.amountMinor);
  if (!d.vendorName || d.invoiceNumber === undefined || d.invoiceNumber === '' || !Number.isInteger(amount) || !/^[A-Za-z]{3}$/.test(String(d.currency))) return null;
  return {
    action: d.action,
    facts: { vendorName: String(d.vendorName), invoiceNumber: String(d.invoiceNumber), amountMinor: amount, currency: String(d.currency).toUpperCase() },
    evidenceRefs: refs,
    reason: String(d.reason || '').slice(0, 700),
  };
}

async function modelDecide(env, pkgs) {
  const res = new Array(pkgs.length).fill(null);
  const run = async (idxs, ms) => {
    const ds = await askModel(env, idxs.map((i) => pkgs[i]), ms).catch((e) => { console.error('model', e && e.message); return []; });
    idxs.forEach((i, j) => { const v = validate(ds.find((x) => x && x.i === j), pkgs[i]); if (v) res[i] = v; });
  };
  const all = pkgs.map((_, i) => i);
  const chunks = [];
  for (let i = 0; i < all.length; i += 12) chunks.push(all.slice(i, i + 12));
  await Promise.all(chunks.map((c) => run(c, 18000)));            // first pass, one call per 12 packages
  const failed = all.filter((i) => !res[i]);
  if (failed.length) await run(failed.slice(0, 12), 8000);        // one batched retry, then fallback
  return res;
}

const SYSTEM = `You are an accounts-payable analyst. For each invoice package choose exactly one action:
- settle_invoice: valid, reconciled, and within autonomous payment authority.
- request_approval: commercially valid, but amount or terms are outside delegated authority; an approver must sign off.
- hold_invoice: payment pauses until a stated verification completes (for example bank-detail callback, missing receipt/PO, tax or vendor check).
- reject_duplicate: the SAME commercial invoice (same vendor and invoice number) was already paid. A payment for a DIFFERENT invoice is NOT a duplicate.
- open_exception: material records conflict (invoice vs PO vs receipt vs contract, amount, currency, vendor identity) and need an exception workflow.
Each package mixes the real case with noise: a cover sheet, archive examples, old cases, training decoys, negated statements ("do not hold...") and irrelevant action words. Find the ONE paragraph that actually determines the action for THIS invoice. evidenceRefs must be exactly the three bracketed references (like "[ABC-123]") that appear in that decisive paragraph, copied character for character. Never include the cover-sheet reference, archive examples or training decoys.
Facts for the invoice being decided: vendorName, invoiceNumber, currency (ISO code) and amountMinor (integer minor units, e.g. 12,345.67 INR -> 1234567).
reason: at most 20 words explaining the action.
Return JSON only: {"decisions":[{"i":<package index>,"action":...,"vendorName":...,"invoiceNumber":...,"amountMinor":...,"currency":...,"evidenceRefs":[...],"reason":...}]} with one entry per package.`;

const SCHEMA = {
  type: 'object',
  properties: {
    decisions: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          i: { type: 'integer' },
          action: { type: 'string', enum: ACTIONS },
          vendorName: { type: 'string' },
          invoiceNumber: { type: 'string' },
          amountMinor: { type: 'integer' },
          currency: { type: 'string' },
          evidenceRefs: { type: 'array', items: { type: 'string' } },
          reason: { type: 'string' },
        },
        required: ['i', 'action', 'vendorName', 'invoiceNumber', 'amountMinor', 'currency', 'evidenceRefs', 'reason'],
      },
    },
  },
  required: ['decisions'],
};

function parseJson(r) {
  if (r && typeof r === 'object') return r;
  const s = String(r || '');
  return JSON.parse(s.slice(s.indexOf('{'), s.lastIndexOf('}') + 1));
}

async function askModel(env, pkgs, ms) {
  const messages = [
    { role: 'system', content: SYSTEM },
    { role: 'user', content: pkgs.map((p, i) => `### PACKAGE ${i}\n${render(p)}`).join('\n\n') + `\n\nReturn decisions for packages 0..${pkgs.length - 1}.` },
  ];
  const call = env.LLM_URL ? externalModel(env, messages) : workersAI(env, messages);
  const out = await Promise.race([call, new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);
  return out.decisions || [];
}

async function workersAI(env, messages) {
  const model = env.MODEL || '@cf/meta/llama-3.3-70b-instruct-fp8-fast';
  const base = { messages, max_tokens: 2048, temperature: 0 };
  try {
    const r = await env.AI.run(model, { ...base, response_format: { type: 'json_schema', json_schema: SCHEMA } });
    return parseJson(r.response);
  } catch {
    const r = await env.AI.run(model, base);
    return parseJson(r.response);
  }
}

// Optional: any OpenAI-compatible endpoint (set LLM_URL, LLM_KEY, LLM_MODEL as secrets/vars).
async function externalModel(env, messages) {
  const r = await fetch(env.LLM_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Bearer ' + env.LLM_KEY },
    body: JSON.stringify({ model: env.LLM_MODEL, messages, temperature: 0, response_format: { type: 'json_object' } }),
  });
  const j = await r.json();
  return parseJson(j.choices[0].message.content);
}
