module.exports = {
  roots: ["<rootDir>/tests/contract", "<rootDir>/tests/guardrail"],
  testMatch: ["**/*.test.js"],
  collectCoverage: true,
  coverageDirectory: "coverage",
};