module.exports = {
  preset: 'ts-jest', // enables TS support
  testEnvironment: 'node', // good for backend/API tests
  transform: {
    '^.+\\.(ts|js)$': 'ts-jest', // handle both JS & TS
  },
  testMatch: [
    '**/__tests__/**/*.+(ts|js|tsx|jsx)',
    '**/?(*.)+(spec|test).+(ts|js|tsx|jsx)'
  ],
  moduleFileExtensions: ['ts', 'js', 'json', 'node'],
  verbose: true, // nice to see test results in detail
};