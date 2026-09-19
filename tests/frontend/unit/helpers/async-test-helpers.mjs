// Shared async/timer/flush stubs for frontend unit tests (W1b consolidation).

export async function flushBackgroundTasks() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

export async function flushMicrotasksWithTimer(count = 5) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
  await new Promise(resolve => setTimeout(resolve, 0));
}

export async function flushMicrotaskPair() {
  await Promise.resolve();
  await Promise.resolve();
}

export function createDeferred() {
  let resolve;
  const promise = new Promise(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

export function createJsonResponse(payload) {
  return {
    ok: true,
    json: async () => payload
  };
}

export function createDeferredWithReject() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

export async function flushMicrotasks(count = 10) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}
