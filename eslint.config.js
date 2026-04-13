import js from "@eslint/js";

export default [
  {
    ignores: [
      ".tmp/**",
      "_out/**",
      ".pre-commit-home*/**",
      "node_modules/**",
      "data/**",
      "dist/**",
      "docs/**",
      "playwright-report/**",
      "pytest-cache-files-*/**",
      "tmp*/**",
    ],
  },

  js.configs.recommended,

  {
    files: [
      "frontend/**/*.js",
      "tests/**/*.js",
      "tests/**/*.mjs",
      "scripts/**/*.mjs",
    ],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        console: "readonly",
        window: "readonly",
        document: "readonly",
        localStorage: "readonly",
        sessionStorage: "readonly",
        indexedDB: "readonly",
        fetch: "readonly",
        URL: "readonly",
        URLSearchParams: "readonly",
        FormData: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        navigator: "readonly",
        HTMLInputElement: "readonly",
        performance: "readonly",
        FileReader: "readonly",
        Blob: "readonly",
        alert: "readonly",
        location: "readonly",
        history: "readonly",
        requestAnimationFrame: "readonly",
        Element: "readonly",
        HTMLElement: "readonly",
        HTMLTextAreaElement: "readonly",
        process: "readonly",
        AbortController: "readonly",
        TextEncoder: "readonly",
        TextDecoder: "readonly",
        IDBKeyRange: "readonly",
        atob: "readonly",
        btoa: "readonly",
      },
    },
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" }],
      "no-undef": "error",
      "no-console": "off",
    },
  },

  {
    files: ["tests/**/*.js", "tests/**/*.mjs"],
    languageOptions: {
      globals: {
        describe: "readonly",
        it: "readonly",
        test: "readonly",
        expect: "readonly",
        before: "readonly",
        beforeEach: "readonly",
        after: "readonly",
        afterEach: "readonly",
        global: "readonly",
        Blob: "readonly",
        setImmediate: "readonly",
      },
    },
    rules: {
      "no-unused-expressions": "off",
    },
  },

  // Root-level JS files (browser scripts)
  {
    files: ["theme.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "script",
      globals: {
        console: "readonly",
        window: "readonly",
        document: "readonly",
        localStorage: "readonly",
        _: "readonly",
        alert: "readonly",
      },
    },
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" }],
      "no-undef": "error",
    },
  },

  // Root-level ES module files
  {
    files: [
      "eslint.config.js",
      "frontend-runtime-config.js",
      "playwright.config.js",
    ],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        console: "readonly",
        window: "readonly",
        document: "readonly",
        localStorage: "readonly",
        sessionStorage: "readonly",
        fetch: "readonly",
        URL: "readonly",
        URLSearchParams: "readonly",
        FormData: "readonly",
        navigator: "readonly",
        history: "readonly",
        location: "readonly",
        _: "readonly",
        alert: "readonly",
        process: "readonly",
        Blob: "readonly",
        FileReader: "readonly",
        TextEncoder: "readonly",
        TextDecoder: "readonly",
        AbortController: "readonly",
        atob: "readonly",
        btoa: "readonly",
        IDBKeyRange: "readonly",
        indexedDB: "readonly",
        performance: "readonly",
      },
    },
    rules: {
      "no-unused-vars": "warn",
      "no-undef": "error",
    },
  },

  // Probe scripts (browser environment)
  {
    files: ["probes/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        console: "readonly",
        window: "readonly",
        document: "readonly",
        fetch: "readonly",
        URL: "readonly",
        performance: "readonly",
      },
    },
    rules: {
      "no-unused-vars": "warn",
      "no-undef": "error",
    },
  },
];
