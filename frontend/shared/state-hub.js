/** Cross-module state snapshot holder. Only `set` is used; values are read via direct module imports where needed. */

const state = {};

/**
 * @param {string} key
 * @param {unknown} value
 */
export function set(key, value) {
  state[key] = value;
}
