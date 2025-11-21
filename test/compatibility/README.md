# Compatibility Tests

This directory contains test projects to verify that `iceberg-js` works in all common Node.js environments.

## Test Scenarios

1. **esm-project/** - Pure JavaScript ESM (`"type": "module"`)
2. **cjs-project/** - Pure JavaScript CommonJS (no type field)
3. **ts-esm-project/** - TypeScript with ESM (`module: "ESNext"`)
4. **ts-cjs-project/** - TypeScript with CommonJS (`module: "CommonJS"`)

## Running Tests

From the root of the `iceberg-js` project:

```bash
# Build the package first
pnpm build

# Run all compatibility tests
bash test/compatibility/run-all.sh
```

Or test individually:

```bash
cd test/compatibility/esm-project
npm install
npm test
```

## What Gets Tested

- ✅ Package can be imported/required
- ✅ Classes and types are available
- ✅ Instances can be created
- ✅ TypeScript types resolve correctly
- ✅ No module resolution errors
