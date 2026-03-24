import js from "@eslint/js";

export default [
  {
    ignores: [
      "node_modules/**",
      "data/**",
      "docs/**",
      "playwright-report/**",
      "test-results/**",
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
        indexedDB: "readonly",
        fetch: "readonly",
        URL: "readonly",
        URLSearchParams: "readonly",
        FormData: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
      },
    },
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
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
      },
    },
    rules: {
      "no-unused-expressions": "off",
    },
  },
];