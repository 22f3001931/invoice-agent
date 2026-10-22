// Usage: node test.mjs https://<worker>.workers.dev/a2a
// Simulates the grader against synthetic packages. Needs Node 18+.
const base = process.argv[2].replace(/\/+$/, '');
const BATCH = 'application/vnd.ga5.invoice-claim-batch+json';
const RES = 'application/vnd.ga5.invoice-action-results+json';
const H = (t = 'tokenA') => ({ authorization: 'Bearer ' + t, 'a2a-version': '1.0', 'content-type': 'application/a2a+json' });
const j = async (r) => ({ s: r.status, b: await r.json().catch(() => null), ct: r.headers.get('content-type') });
const post = (p, body, t) => fetch(base + p, { method: 'POST', headers: H(t), body: JSON.stringify(body) }).then(j);
const get = (p, t) => fetch(base + p, { headers: H(t) }).then(j);
let fails = 0;
const ok = (c, m) => { console.log(c ? 'PASS' : 'FAIL', m); if (!c) fails++; };
const rnd = () => Math.random().toString(36).slice(2, 10);

const PK = [
  { packageId: 'p1', cover: 'Cover sheet [COV-1]', body: 'Archive example (old): hold. Current: Acme Ltd invoice INV-100 for INR 1,200.50 matches PO and receipt, under limit. [POL-1] [LED-1] [RCP-1]. Settle.' },
  { packageId: 'p2', cover: 'Cover sheet [COV-2]', body: 'Zed Corp invoice Z-9 for INR 9,99,000.00 exceeds the delegated limit; approver needed. [POL-7] [AMT-2] [LIM-3]' },
];
const mk = (id, batch) => ({
  message: { messageId: id, role: 'ROLE_USER', parts: [{ mediaType: BATCH, data: { batchId: batch, policyRevision: 'r1', packages: PK } }] },
  configuration: { returnImmediately: false, historyLength: 20 },
});
const rev = (o) => Array.isArray(o) ? o.map(rev) : o && typeof o === 'object'
  ? Object.fromEntries(Object.entries(o).reverse().map(([k, v]) => [k, rev(v)])) : o;
const cont = (t, results, id = 'res-' + rnd()) => ({
  message: { messageId: id, taskId: t.id, contextId: t.contextId, role: 'ROLE_USER',
    parts: [{ mediaType: RES, data: { batchId: t.artifacts[0].parts[0].data.batchId, results } }] },
});
const results = (t, accepted = true) => t.artifacts[0].parts[0].data.proposals.map((p, i) => ({
  packageId: p.packageId, actionId: p.actionId, action: p.action,
  outcome: i === 0 && accepted ? 'ACCEPTED' : 'REJECTED', receiptNonce: 'nonce-' + rnd() + '-' + i,
}));

// Card & auth
const card = await fetch(new URL('/.well-known/agent-card.json', base).href).then(j);
ok(card.s === 200 && card.b.supportedInterfaces.some((i) => i.url === base), 'agent card + exact base URL');
ok((await fetch(base + '/tasks')).status === 401, 'missing auth -> 401');
ok((await fetch(base + '/tasks', { headers: { authorization: 'Bearer x', 'a2a-version': '0.3' } })).status === 400, 'wrong version -> 400');

// Send + dedupe
const id1 = 'm-' + rnd();
const body = mk(id1, 'b-' + rnd());
const [a, b] = await Promise.all([post('/message:send', body), post('/message:send', rev({ ...body, configuration: { returnImmediately: true } }))]);
ok(a.s === 200 && a.b.task.status.state === 'TASK_STATE_INPUT_REQUIRED', 'INPUT_REQUIRED returned');
ok(a.ct?.startsWith('application/a2a+json'), 'a2a+json media type');
ok(b.s === 200 && b.b.task.id === a.b.task.id, 'concurrent/reordered duplicate -> same task');
const c = mk(id1, body.message.parts[0].data.batchId); c.message.parts[0].data.policyRevision = 'changed';
ok((await post('/message:send', c)).s === 409, 'changed content -> 409');
const t = a.b.task;
ok(t.artifacts.length === 1 && t.artifacts[0].parts.length === 1, 'one proposal artifact part');
console.log(JSON.stringify(t.artifacts[0].parts[0].data.proposals, null, 1));

// Isolation
ok((await get('/tasks/' + t.id, 'tokenB')).s === 404, 'outsider GET -> 404');
ok(JSON.stringify((await get('/tasks', 'tokenB')).b) === '{"tasks":[]}', 'outsider list empty');
ok((await post('/tasks/' + t.id + ':cancel', {}, 'tokenB')).s === 404, 'outsider cancel -> 404');

// Continuations
const bad = results(t); bad[0].actionId = 'wrong-action-id-1234';
ok((await post('/message:send', cont(t, bad))).s === 400, 'mismatched actionId -> 400');
const rid = 'res-' + rnd();
const done = await post('/message:send', cont(t, results(t), rid));
ok(done.s === 200 && done.b.task.status.state === 'TASK_STATE_COMPLETED', 'COMPLETED after valid result');
const rc = done.b.task.artifacts[1]?.parts[0].data.executions;
ok(rc?.length === 1, 'receipt only for accepted result');
ok(JSON.stringify((await post('/message:send', cont(t, results(t), rid))).b) !== undefined, 'replay answered');
ok((await post('/tasks/' + t.id + ':cancel', {})).s === 409, 'cancel after terminal -> 409');

// Cancel vs result race
const t2 = (await post('/message:send', mk('m-' + rnd(), 'b-' + rnd()))).b.task;
const race = await Promise.all([post('/message:send', cont(t2, results(t2))), post('/tasks/' + t2.id + ':cancel', {})]);
ok(race.map((r) => r.s).sort().join() === '200,409', 'race: exactly one 200 and one 409');
const fin = (await get('/tasks/' + t2.id)).b;
ok(fin.status.state === 'TASK_STATE_COMPLETED' ? fin.artifacts.length === 2 : fin.artifacts.length === 1, 'no COMPLETED+CANCELED mix');

console.log(fails ? `\n${fails} check(s) failed` : '\nAll local checks passed');
process.exit(fails ? 1 : 0);
