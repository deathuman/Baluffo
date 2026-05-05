import test from "node:test";
import assert from "node:assert/strict";

import { observeLongTasks } from "../../../probes/long-task-observer.js";

function createObserverClass({ supportedEntryTypes = ["longtask"] } = {}) {
  const instances = [];
  class FakePerformanceObserver {
    static supportedEntryTypes = supportedEntryTypes;

    constructor(callback) {
      this.callback = callback;
      this.observeCalls = [];
      this.disconnected = false;
      instances.push(this);
    }

    observe(options) {
      this.observeCalls.push(options);
    }

    disconnect() {
      this.disconnected = true;
    }

    emit(entries) {
      this.callback({
        getEntries() {
          return entries;
        }
      });
    }
  }
  return { Observer: FakePerformanceObserver, instances };
}

test("long task observer emits normalized metrics for multiple entries", () => {
  const emitted = [];
  const { Observer, instances } = createObserverClass();

  observeLongTasks({
    page: "admin",
    performanceObserver: Observer,
    emitMetric: (event, payload) => emitted.push({ event, payload })
  });

  instances[0].emit([
    {
      duration: 51.7,
      startTime: 14.2,
      name: "self",
      entryType: "longtask",
      attribution: [{
        name: "script",
        duration: 12.1,
        containerType: "window",
        containerName: "admin",
        containerId: "root"
      }]
    },
    {
      duration: 88,
      startTime: 40,
      name: "task",
      entryType: "longtask",
      attribution: []
    }
  ]);

  assert.deepEqual(instances[0].observeCalls, [{ type: "longtask", buffered: true }]);
  assert.deepEqual(emitted, [
    {
      event: "admin_long_task",
      payload: {
        durationMs: 52,
        startTimeMs: 14,
        name: "self",
        entryType: "longtask",
        attribution: [{
          name: "script",
          durationMs: 12,
          containerType: "window",
          containerName: "admin",
          containerId: "root"
        }]
      }
    },
    {
      event: "admin_long_task",
      payload: {
        durationMs: 88,
        startTimeMs: 40,
        name: "task",
        entryType: "longtask",
        attribution: []
      }
    }
  ]);
});

test("long task observer sanitizes missing attribution safely", () => {
  const emitted = [];
  const { Observer, instances } = createObserverClass();

  observeLongTasks({
    page: "jobs",
    performanceObserver: Observer,
    emitMetric: (event, payload) => emitted.push({ event, payload })
  });

  instances[0].emit([{ duration: "bad", startTime: null }]);

  assert.deepEqual(emitted, [{
    event: "jobs_long_task",
    payload: {
      durationMs: 0,
      startTimeMs: 0,
      name: "",
      entryType: "longtask",
      attribution: []
    }
  }]);
});

test("long task observer no-ops when PerformanceObserver is unavailable or unsupported", () => {
  const emitted = [];
  const missingHandle = observeLongTasks({
    page: "saved",
    performanceObserver: null,
    emitMetric: (event, payload) => emitted.push({ event, payload })
  });
  const unsupported = createObserverClass({ supportedEntryTypes: ["paint"] });
  const unsupportedHandle = observeLongTasks({
    page: "saved",
    performanceObserver: unsupported.Observer,
    emitMetric: (event, payload) => emitted.push({ event, payload })
  });

  assert.equal(typeof missingHandle.disconnect, "function");
  assert.equal(typeof unsupportedHandle.disconnect, "function");
  assert.equal(unsupported.instances.length, 0);
  assert.deepEqual(emitted, []);
});

test("long task observer disconnect handle disconnects the observer", () => {
  const { Observer, instances } = createObserverClass();

  const handle = observeLongTasks({
    page: "admin",
    performanceObserver: Observer,
    emitMetric: () => {}
  });
  handle.disconnect();

  assert.equal(instances[0].disconnected, true);
});
