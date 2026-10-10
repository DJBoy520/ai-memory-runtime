/**
 * lib/logger.js
 * Lightweight rate-limited logger for openclaw-amr.
 * Fail-safe wrapper with prefix, level filtering, and optional host logger delegation.
 */

const LOG_LEVELS = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export class AmrLogger {
  /**
   * @param {object} [options]
   * @param {string} [options.prefix='[openclaw-amr]']
   * @param {string} [options.level='info']
   * @param {object} [options.hostLogger=null]
   * @param {number} [options.throttleWindowMs=5000]
   */
  constructor(options = {}) {
    this.prefix = options.prefix || "[dsh-amr]";
    this.levelStr = options.level || (process.env.AMR_LOG_LEVEL || "info").toLowerCase();
    this.level = LOG_LEVELS[this.levelStr] || LOG_LEVELS.info;
    this.hostLogger = options.hostLogger || null;
    this.throttleWindowMs = options.throttleWindowMs || 5000;
    this._recentErrors = new Map();
  }

  setHostLogger(logger) {
    if (logger && typeof logger.info === "function") {
      this.hostLogger = logger;
    }
  }

  setLevel(levelStr) {
    if (LOG_LEVELS[levelStr] !== undefined) {
      this.levelStr = levelStr;
      this.level = LOG_LEVELS[levelStr];
    }
  }

  debug(...args) {
    if (this.level <= LOG_LEVELS.debug) {
      if (this.hostLogger && typeof this.hostLogger.debug === "function") {
        this.hostLogger.debug(`${this.prefix} ${args.join(" ")}`);
      } else {
        console.debug(this.prefix, ...args);
      }
    }
  }

  info(...args) {
    if (this.level <= LOG_LEVELS.info) {
      if (this.hostLogger && typeof this.hostLogger.info === "function") {
        this.hostLogger.info(`${this.prefix} ${args.join(" ")}`);
      } else {
        console.info(this.prefix, ...args);
      }
    }
  }

  warn(...args) {
    if (this.level <= LOG_LEVELS.warn) {
      if (this.hostLogger && typeof this.hostLogger.warn === "function") {
        this.hostLogger.warn(`${this.prefix} ${args.join(" ")}`);
      } else {
        console.warn(this.prefix, ...args);
      }
    }
  }

  /**
   * Rate-limited error logging to prevent log spamming during outage/timeout
   * @param {string} key Unique key for throttling
   * @param  {...any} args
   */
  throttledError(key, ...args) {
    if (this.level > LOG_LEVELS.error) return;
    const now = Date.now();
    const last = this._recentErrors.get(key) || 0;
    if (now - last > this.throttleWindowMs) {
      this._recentErrors.set(key, now);
      this.error(...args);
    }
  }

  error(...args) {
    if (this.level <= LOG_LEVELS.error) {
      if (this.hostLogger && typeof this.hostLogger.error === "function") {
        this.hostLogger.error(`${this.prefix} ${args.join(" ")}`);
      } else {
        console.error(this.prefix, ...args);
      }
    }
  }
}

export const logger = new AmrLogger();
