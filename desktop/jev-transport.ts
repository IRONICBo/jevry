import type { JevResponse } from './engine';

const RETRYABLE = new Set([429, 503, 529]);
const DEADLINE_MS = 25_000;
const REPLACEMENT_DELAY_MS = 10_000;
const ERROR_CAPTURE_LIMIT = 4096;
const ERROR_DETAIL_LIMIT = 240;
type Outcome = { kind: 'success'; value: JevResponse } | { kind: 'http'; status: number; detail?: string } |
  { kind: 'error'; error: unknown; phase: 'headers' | 'body' | 'deadline' };

function cancelBody(response?: Response) {
  // Abort cancels an active JSON reader. An unconsumed late response still needs
  // its body cancelled explicitly; never wait for a losing stream to drain.
  if (response?.body && !response.body.locked) void response.body.cancel().catch(() => {});
}

function privateRequestStrings(init: RequestInit): string[] {
  if (typeof init.body !== 'string') return [];
  let value: unknown;
  try { value = JSON.parse(init.body); } catch { return []; }
  if (!value || typeof value !== 'object' || Array.isArray(value)) return [];
  const root = value as Record<string, unknown>;
  const strings: string[] = [], pending = [root.state, root.questions].filter(item => item !== undefined);
  while (pending.length) {
    const item = pending.pop();
    if (typeof item === 'string') {
      const normalized = item.replace(/\s+/g, ' ').trim();
      if (normalized.length >= 4) strings.push(normalized);
    }
    else if (typeof item === 'number' && Number.isFinite(item)) {
      const normalized = String(item);
      if (normalized.length >= 4) strings.push(normalized);
    }
    else if (Array.isArray(item)) pending.push(...item);
    else if (item && typeof item === 'object') pending.push(...Object.values(item));
  }
  return strings;
}

function repeatsPrivateInput(detail: string, init: RequestInit): boolean {
  const authorization = new Headers(init.headers).get('authorization') || '';
  const token = authorization.replace(/^Bearer\s+/i, '');
  if ((authorization && detail.includes(authorization)) || (token && detail.includes(token))) return true;
  for (const source of privateRequestStrings(init)) {
    if (detail.includes(source)) return true;
    for (let index = 0; index <= detail.length - 24; index++) {
      if (source.includes(detail.slice(index, index + 24))) return true;
    }
  }
  return false;
}

async function errorDetail(response: Response, init: RequestInit): Promise<string | undefined> {
  if (!/\b(?:application\/json|[^;\s]+\+json)\b/i.test(response.headers.get('content-type') || '')) {
    cancelBody(response);
    return undefined;
  }
  const reader = response.body?.getReader();
  if (!reader) return undefined;
  const decoder = new TextDecoder();
  let raw = '', size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > ERROR_CAPTURE_LIMIT - size) { await reader.cancel(); return undefined; }
      size += value.byteLength;
      raw += decoder.decode(value, { stream: true });
    }
    raw += decoder.decode();
  } catch { return undefined; }
  finally { reader.releaseLock(); }
  let value: unknown;
  try { value = JSON.parse(raw); } catch { return undefined; }
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const body = value as { detail?: unknown; message?: unknown; error?: unknown };
  const candidate = [body.detail, body.message, body.error,
    body.error && typeof body.error === 'object' && !Array.isArray(body.error) ? (body.error as { message?: unknown }).message : undefined]
    .filter((item): item is string => typeof item === 'string')
    .map(item => item.replace(/[\u0000-\u001f\u007f]+/g, ' ').replace(/\s+/g, ' ').trim())
    .find(Boolean);
  if (!candidate) return undefined;
  const normalized = candidate;
  const bounded = normalized.length <= ERROR_DETAIL_LIMIT ? normalized : `${normalized.slice(0, ERROR_DETAIL_LIMIT - 1)}…`;
  return repeatsPrivateInput(bounded, init) ? undefined : bounded;
}

function pause(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const aborted = () => { clearTimeout(timer); signal?.removeEventListener('abort', aborted); reject(signal?.reason); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', aborted); resolve(); }, ms);
    signal?.addEventListener('abort', aborted, { once: true });
    if (signal?.aborted) aborted();
  });
}

function attempt(endpoint: string, init: RequestInit, signal: AbortSignal | undefined, allowReplacement: boolean,
  overloaded: () => void): Promise<Outcome> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const controllers: AbortController[] = [], responses: Array<Response | undefined> = [];
    const failures: Array<Outcome | undefined> = [];
    const knownHttpFailure = () => failures.find(item => item?.kind === 'http' && !RETRYABLE.has(item.status)) ||
      failures.find(item => item?.kind === 'http');
    let settled = false, pending = 0, primaryHeaders = false;
    let replacementTimer: ReturnType<typeof setTimeout> | undefined;
    const stop = (reason: unknown, winner = -1) => {
      clearTimeout(deadline); clearTimeout(replacementTimer);
      signal?.removeEventListener('abort', callerAborted);
      controllers.forEach((controller, index) => {
        if (index !== winner) { controller.abort(reason); cancelBody(responses[index]); }
      });
    };
    const finish = (outcome: Outcome, winner = -1) => {
      if (settled) return;
      if (signal?.aborted) { callerAborted(); return; }
      settled = true;
      stop(new DOMException('Superseded Jev inference request.', 'AbortError'), winner);
      resolve(outcome);
    };
    const callerAborted = () => {
      if (settled) return;
      settled = true; stop(signal?.reason); reject(signal?.reason);
    };
    const deadline = setTimeout(() => {
      if (settled) return;
      settled = true;
      const error = new DOMException('Jev inference exceeded its 25-second deadline.', 'TimeoutError');
      // A stalled peer must not erase a known HTTP overload's normal backoff.
      stop(error); resolve(knownHttpFailure() || { kind: 'error', error, phase: 'deadline' });
    }, DEADLINE_MS);
    signal?.addEventListener('abort', callerAborted, { once: true });
    const failed = (index: number, outcome: Outcome) => {
      if (settled) return;
      failures[index] = outcome;
      if (--pending) return;
      // A definite HTTP result is more informative than a failed connection.
      // Preserve permanent HTTP errors before considering overload backoff.
      finish(knownHttpFailure() || failures.find(Boolean)!);
    };
    const start = () => {
      const index = controllers.length, controller = new AbortController();
      controllers.push(controller); pending++;
      void (async () => {
        let phase: 'headers' | 'body' = 'headers';
        try {
          const response = await fetch(endpoint, { ...init, signal: controller.signal });
          responses[index] = response;
          if (index === 0) { primaryHeaders = true; clearTimeout(replacementTimer); }
          if (settled) { cancelBody(response); return; }
          if (RETRYABLE.has(response.status)) { overloaded(); clearTimeout(replacementTimer); }
          if (!response.ok) {
            failures[index] = { kind: 'http', status: response.status };
            const detail = RETRYABLE.has(response.status) ? (cancelBody(response), undefined) : await errorDetail(response, init);
            failed(index, { kind: 'http', status: response.status, detail }); return;
          }
          phase = 'body';
          const value = await response.json() as JevResponse;
          if (!value || !value.answers || typeof value.answers !== 'object' || Array.isArray(value.answers)) {
            throw new Error('Jev returned an invalid answer. No action executed.');
          }
          finish({ kind: 'success', value }, index);
        } catch (error) { failed(index, { kind: 'error', error, phase }); }
      })();
    };
    if (signal?.aborted) { callerAborted(); return; }
    start();
    if (allowReplacement && !settled) replacementTimer = setTimeout(() => {
      if (!settled && !primaryHeaders) start();
    }, REPLACEMENT_DELAY_MS);
  });
}

/** Read-only inference transport; never encloses or replays browser input.
 * Existing overload backoff is documented at https://docs.typesafe.ai/api.
 * The delayed replacement is a local bounded experiment, not an API feature or
 * a proven speed improvement. A slow successful primary remains eligible.
 */
export async function fetchJevInference(endpoint: string, init: RequestInit, signal?: AbortSignal): Promise<JevResponse> {
  let sawOverload = false;
  for (let index = 0; index < 3; index++) {
    signal?.throwIfAborted();
    const outcome = await attempt(endpoint, init, signal, index === 0 && !sawOverload, () => { sawOverload = true; });
    signal?.throwIfAborted();
    if (outcome.kind === 'success') return outcome.value;
    if (outcome.kind === 'http') {
      if (RETRYABLE.has(outcome.status) && index < 2) { await pause(300 * 2 ** index, signal); continue; }
      if (outcome.detail) throw new Error(`Jev returned HTTP ${outcome.status}: ${outcome.detail} No action executed.`);
      throw new Error(`Jev returned HTTP ${outcome.status}. Check your key, endpoint, and model. No action executed.`);
    }
    if (outcome.phase === 'body') throw outcome.error;
    throw new Error(outcome.phase === 'deadline' ? 'Jev timed out. No action executed.' : 'Could not connect to Jev. No action executed.', { cause: outcome.error });
  }
  throw new Error('Jev is temporarily unavailable. No action executed.');
}
