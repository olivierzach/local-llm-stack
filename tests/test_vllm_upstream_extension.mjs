import assert from 'node:assert/strict';
import { mkdtemp, mkdir, copyFile, writeFile, rm, realpath, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { after, test } from 'node:test';

// Node 24 strips the extension's type-only imports. Exercise an unchanged copy
// with isolated evidence; never read or mutate the live watcher's/session state.
const root = await realpath(await mkdtemp(join(tmpdir(), 'vllm-watch-guard-')));
after(() => rm(root, { recursive: true, force: true }));
const extension = join(root, '.omp/extensions/vllm-upstream.ts');
const watch = join(root, 'logs/incidents/2026-10-01-vllm-worker-failure/upstream-watch');
await mkdir(join(root, '.omp/extensions'), { recursive: true });
await mkdir(watch, { recursive: true });
await copyFile(new URL('../.omp/extensions/vllm-upstream.ts', import.meta.url), extension);
const request = {
  schema_version: 1, event_id: 'a'.repeat(64), observed_at: '2026-10-08T09:34:04Z',
  summary: 'Discussion changed', prs: [53597, 58738],
};
await writeFile(join(watch, 'triage-request.json'), JSON.stringify(request), { mode: 0o600 });
await writeFile(join(watch, 'snapshot.json'), '{}');
const { default: install } = await import(pathToFileURL(extension).href);
const stateType = 'local-llm-stack.vllm-upstream.state';

async function harness(t) {
  const handlers = new Map(), tools = new Map(), timers = new Map();
  const messages = [], entries = [];
  const sessionId = crypto.randomUUID();
  let timerId = 0, aborted = false;
  entries.push({ type: 'custom', customType: stateType, data: {
    schema_version: 1, session_id: sessionId, subscribed: true,
  } });
  const ctx = {
    sessionManager: { getSessionId: () => sessionId, getEntries: () => entries },
    ui: { notify() {} }, isIdle: () => true, hasPendingMessages: () => false,
    setInterval(fn, ms) { timers.set(++timerId, { fn, ms }); return timerId; },
    setTimeout(fn, ms) { timers.set(++timerId, { fn, ms }); return timerId; },
    clearTimer(id) { timers.delete(id); },
    async abort() { aborted = true; },
  };
  install({
    on: (event, fn) => handlers.set(event, fn),
    registerTool: tool => tools.set(tool.name, tool), registerCommand() {},
    appendEntry: (customType, data) => entries.push({ type: 'custom', customType, data }),
    sendMessage: async message => { messages.push(message); },
    zod: { object: value => value, string: () => ({}) },
  });
  t.after(() => handlers.get('session_shutdown')());
  handlers.get('session_start')({}, ctx);
  const tick = ms => [...timers.values()].find(timer => timer.ms === ms).fn();
  await tick(15_000);
  assert.equal(messages.length, 1, 'subscribed idle session starts one triage');
  return {
    handlers, tools, ctx, messages, tick, aborted: () => aborted,
    call: (toolName, path) => handlers.get('tool_call')({ toolName, input: { path } }),
  };
}

test('ACK device transport works without consuming the evidence-read allowance', async t => {
  const h = await harness(t);
  for (let i = 0; i < 13; i++) {
    assert.equal(h.call('read', 'xd://vllm_upstream_ack'), undefined);
    assert.equal(h.call('write', 'xd://vllm_upstream_ack'), undefined);
    assert.equal(h.call('vllm_upstream_ack'), undefined);
  }
  for (let i = 0; i < 12; i++) {
    assert.equal(h.call('read', join(watch, 'snapshot.json')), undefined);
  }
  assert.equal(h.call('read', join(watch, 'snapshot.json')).block, true);
});

test('the ACK exception does not authorize other tools, devices, writes or path variants', async t => {
  const h = await harness(t);
  for (const [tool, path] of [
    ['write', join(watch, 'snapshot.json')], ['write', 'xd://lsp'], ['read', 'xd://lsp'],
    ['write', 'xd://vllm_upstream_ack:raw'], ['read', 'xd://vllm_upstream_ack?x=1'],
    ['grep', 'xd://vllm_upstream_ack'], ['bash', undefined],
  ]) assert.equal(h.call(tool, path).block, true, `${tool} ${path}`);
});

test('evidence access rejects compound paths, redirects and missing-path fallback', async t => {
  const h = await harness(t);
  const patches = join(root, 'patches/vllm-remote-worker-failure/direct-exit');
  await mkdir(patches, { recursive: true });
  const evidence = join(patches, 'verification.json');
  const outside = join(root, 'private.txt');
  await writeFile(evidence, '{}');
  await writeFile(outside, 'private fixture');
  const link = join(patches, 'private-link');
  await symlink(outside, link);
  for (const [tool, path] of [
    ['grep', `${patches}, ${outside}`],
    ['grep', `${patches} ${outside}`],
    ['glob', `${patches}/{verification.json,../../../../private.txt}`],
    ['read', `${patches}/missing/private.txt`],
    ['read', link],
    ['read', `${patches}/%2E%2E/%2E%2E/%2E%2E/private.txt`],
    ['read', `${evidence}:/private.txt`],
  ]) assert.equal(h.call(tool, path)?.block, true, `${tool} ${path}`);
  assert.equal(h.call('read', `${evidence}:1-2`), undefined);
  assert.equal(h.call('glob', patches), undefined);
});

test('ACK validates event/findings and prevents replay after session restore', async t => {
  const h = await harness(t);
  const ack = h.tools.get('vllm_upstream_ack');
  const findings = { event_id: request.event_id, evidence: 'Reviewed exact heads',
    coordination: 'Author direction agreed', testing: 'No source changes' };
  const execute = params => ack.execute('smoke', params, undefined, undefined, h.ctx);
  await assert.rejects(execute({ ...findings, event_id: 'b'.repeat(64) }), /exact pending attempt/);
  await assert.rejects(execute({ ...findings, testing: '' }), /three completed findings/);
  assert.equal(h.call('write', 'xd://vllm_upstream_ack'), undefined);
  assert.equal((await execute(findings)).details.acknowledged, true);
  h.handlers.get('agent_end')();
  h.handlers.get('session_start')({}, h.ctx);
  await h.tick(15_000);
  assert.equal(h.messages.length, 1, 'acknowledged observation is not replayed');
});

test('automatic triage still aborts at its deadline', async t => {
  const h = await harness(t);
  await h.tick(120_000);
  assert.equal(h.aborted(), true);
});
