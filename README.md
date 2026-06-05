# iceberg-js

[![GitHub](https://img.shields.io/badge/GitHub-iceberg--js-181717?logo=github)](https://github.com/supabase/iceberg-js)
[![Docs](https://img.shields.io/badge/docs-API%20Reference-blue?logo=readthedocs)](https://supabase.github.io/iceberg-js/)
[![License](https://img.shields.io/npm/l/nx.svg?style=flat-square)](./LICENSE)
[![CI](https://github.com/supabase/iceberg-js/actions/workflows/ci.yml/badge.svg)](https://github.com/supabase/iceberg-js/actions/workflows/ci.yml)
[![npm version](https://badge.fury.io/js/iceberg-js.svg)](https://www.npmjs.com/package/iceberg-js)
[![pkg.pr.new](https://pkg.pr.new/badge/supabase/iceberg-js)](https://pkg.pr.new/~/supabase/iceberg-js)

A small, framework-agnostic JavaScript/TypeScript client for the **Apache Iceberg REST Catalog**.

Tracks the OpenAPI spec at `apache-iceberg-1.11.0-rc1`. The exact tag is exported as `ICEBERG_REST_SPEC_TAG` and is the source of truth for the conformance tests in CI — see [`spec-pin.json`](./spec-pin.json).

## Motivation

This library provides JavaScript and TypeScript developers with a straightforward way to interact with Apache Iceberg REST Catalogs. It's designed as a thin HTTP wrapper that mirrors the official REST API, making it easy to manage namespaces and tables from any JS/TS environment.

## Goals

- **REST API wrapper**: Provide a 1:1 mapping to the Iceberg REST Catalog API
- **Type safety**: Full TypeScript support with strongly-typed request/response models
- **Minimal footprint**: No engine-specific logic, no heavy dependencies
- **Stability**: Production-ready for catalog management operations
- **Vendor-agnostic**: Works with any Iceberg REST Catalog implementation

## Non-Goals

This library intentionally does **not** support:

- **Data operations**: Reading or writing table data (Parquet files, etc.)
- **Query execution**: Use dedicated query engines (Spark, Trino, DuckDB, etc.)
- **Engine integration**: No Spark, Flink, or other engine-specific code
- **Advanced features**: Branching, tagging, time travel queries beyond metadata
- **Views or multi-table transactions**

These boundaries keep the library focused and maintainable. For data operations, pair this library with a query engine that supports Iceberg.

## Features

- **Generic**: Works with any Iceberg REST Catalog implementation, not tied to any specific vendor
- **Minimal**: Thin HTTP wrapper over the official REST API, no engine-specific logic
- **Type-safe**: First-class TypeScript support with strongly-typed request/response models
- **Fetch-based**: Uses native `fetch` API with support for custom implementations
- **Universal**: Targets Node 22+ and modern browsers (ES2020)
- **Catalog-only**: Focused on catalog operations (no data reading/Parquet support in v1.0)

## Documentation

📚 **Full API documentation**: [supabase.github.io/iceberg-js](https://supabase.github.io/iceberg-js/)

## Installation

```bash
npm install iceberg-js
```

## Migrating from 0.x to 1.0

The 1.0 release aligns the client with the [Apache Iceberg REST Catalog OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml). Breaking changes are limited to the listing methods, the `updateTable` body shape, and the `accessDelegation` flow (use the new `loadTableResult` / `createTableResult` to receive vended credentials).

**See [`MIGRATION.md`](./MIGRATION.md) for full before / after code blocks for every change.** The cheat sheet:

| Before (0.x)                                                                 | After (1.0)                                                                                                                                                                  |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `await catalog.listNamespaces()` returns `NamespaceIdentifier[]`             | returns `{ namespaces: NamespaceIdentifier[]; nextPageToken? }`                                                                                                              |
| `await catalog.listNamespaces({ namespace: ['x'] })` (parent)                | `await catalog.listNamespaces({ parent: { namespace: ['x'] } })`                                                                                                             |
| `await catalog.listTables({ namespace: ['x'] })` returns `TableIdentifier[]` | returns `{ identifiers: TableIdentifier[]; nextPageToken? }`                                                                                                                 |
| `updateTable({ properties: { … } })`                                         | `updateTable({ requirements: [], updates: [{ action: 'set-properties', updates: { … } }] })`                                                                                 |
| `catalogName` builds the path manually as `/v1/<name>/...`                   | `warehouse` (or `catalogName` as alias) goes through `GET /v1/config` and uses the server-returned `prefix`. Closes [#32](https://github.com/supabase/iceberg-js/issues/32). |
| `loadTable(id)` returned credentials inline when `accessDelegation` was set  | `loadTableResult(id)` returns credentials + config + ETag; `loadTable(id)` returns bare `TableMetadata`                                                                      |
| Supported Node 18+                                                           | Now requires Node 22+ (Node 18 and 20 are EOL or near-EOL). Drop in [#46](https://github.com/supabase/iceberg-js/pull/46).                                                   |

Non-breaking additions in 1.0:

- `Idempotency-Key` is automatically emitted on every POST/DELETE mutation.
- `loadTable(id, { ifNoneMatch })` enables conditional GETs (returns `null` on 304).
- `loadTable(id, { snapshots: 'all' | 'refs' })` controls how many snapshots the server includes.
- `loadTableResult` / `createTableResult` / `registerTableResult` return the full spec-shaped `LoadTableResult` plus the response `ETag`.
- `registerTable`, `renameTable`, `updateNamespaceProperties`, `commitTable` (alias for `updateTable`), and `loadConfig` are now exposed.
- The full `TableUpdate` and `TableRequirement` discriminated unions are exported.
- Network-level errors (DNS, TLS, offline) now surface as `IcebergError` with `status: 0`, so a single `instanceof IcebergError` check catches every failure mode.

## Quick Start

```typescript
import { IcebergRestCatalog } from 'iceberg-js'

const catalog = new IcebergRestCatalog({
  baseUrl: 'https://my-catalog.example.com',
  warehouse: 'my-warehouse', // optional; resolved via GET /v1/config
  auth: {
    type: 'bearer',
    token: process.env.ICEBERG_TOKEN,
  },
})

// Create a namespace
await catalog.createNamespace({ namespace: ['analytics'] })

// List namespaces (paginated)
const { namespaces, nextPageToken } = await catalog.listNamespaces({ pageSize: 50 })

// Create a table
await catalog.createTable(
  { namespace: ['analytics'] },
  {
    name: 'events',
    schema: {
      type: 'struct',
      fields: [
        { id: 1, name: 'id', type: 'long', required: true },
        { id: 2, name: 'timestamp', type: 'timestamp', required: true },
        { id: 3, name: 'user_id', type: 'string', required: false },
      ],
      'schema-id': 0,
      'identifier-field-ids': [1],
    },
    'partition-spec': {
      'spec-id': 0,
      fields: [],
    },
    'write-order': {
      'order-id': 0,
      fields: [],
    },
    properties: {
      'write.format.default': 'parquet',
    },
  }
)
```

## API Reference

### Constructor

#### `new IcebergRestCatalog(options)`

Creates a new catalog client instance.

**Options:**

- `baseUrl` (string, required): Base URL of the REST catalog
- `warehouse` (string, optional): Warehouse identifier. On first use, the client calls `GET /v1/config?warehouse=…` and uses the server-returned `overrides.prefix` for all subsequent requests. This is the spec-recommended pattern (see [Apache Iceberg REST spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)) and is the way to address per-warehouse catalogs such as Cloudflare R2 or Tabular.
- `catalogName` (string, optional): Permanent alias for `warehouse`, kept for backward compatibility. If both are provided, `warehouse` wins.
- `auth` (AuthConfig, optional): Authentication configuration
- `fetch` (typeof fetch, optional): Custom fetch implementation
- `accessDelegation` (AccessDelegation[], optional): Access delegation mechanisms to request from the server

**Authentication types:**

```typescript
// No authentication
{ type: 'none' }

// Bearer token
{ type: 'bearer', token: 'your-token' }

// Custom header
{ type: 'header', name: 'X-Custom-Auth', value: 'secret' }

// Custom function
{ type: 'custom', getHeaders: async () => ({ 'Authorization': 'Bearer ...' }) }
```

**Access Delegation:**

Access delegation allows the catalog server to provide temporary credentials or sign requests on your behalf:

```typescript
import { IcebergRestCatalog } from 'iceberg-js'

const catalog = new IcebergRestCatalog({
  baseUrl: 'https://catalog.example.com/iceberg/v1',
  auth: { type: 'bearer', token: 'your-token' },
  // Request vended credentials for data access
  accessDelegation: ['vended-credentials'],
})

// To access vended credentials (storage-credentials, server config), use the
// *Result variants — `loadTable`/`createTable`/`registerTable` return only
// the bare `TableMetadata` and would discard credentials.
const result = await catalog.loadTableResult({ namespace: ['analytics'], name: 'events' })
// result['storage-credentials'], result.config, result.etag, result.metadata
```

Supported delegation mechanisms:

- `vended-credentials`: Server provides temporary credentials (e.g., AWS STS tokens) for accessing table data
- `remote-signing`: Server signs data access requests on behalf of the client

### Namespace Operations

#### `listNamespaces(options?): Promise<{ namespaces, nextPageToken? }>`

List namespaces, optionally under a parent namespace, with cursor-based pagination.

```typescript
const { namespaces } = await catalog.listNamespaces()
// namespaces: [{ namespace: ['default'] }, { namespace: ['analytics'] }]

const { namespaces: children } = await catalog.listNamespaces({
  parent: { namespace: ['analytics'] },
})

// Pagination
const page1 = await catalog.listNamespaces({ pageSize: 100 })
const page2 = await catalog.listNamespaces({
  pageSize: 100,
  pageToken: page1.nextPageToken,
})
```

#### `createNamespace(id: NamespaceIdentifier, metadata?: NamespaceMetadata): Promise<void>`

Create a new namespace with optional properties.

```typescript
await catalog.createNamespace({ namespace: ['analytics'] }, { properties: { owner: 'data-team' } })
```

#### `dropNamespace(id: NamespaceIdentifier): Promise<void>`

Drop a namespace. The namespace must be empty.

```typescript
await catalog.dropNamespace({ namespace: ['analytics'] })
```

#### `loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata>`

Load namespace metadata and properties.

```typescript
const metadata = await catalog.loadNamespaceMetadata({ namespace: ['analytics'] })
// { properties: { owner: 'data-team', ... } }
```

#### `updateNamespaceProperties(id, request): Promise<UpdateNamespacePropertiesResponse>`

Set or remove namespace properties.

```typescript
await catalog.updateNamespaceProperties(
  { namespace: ['analytics'] },
  { updates: { owner: 'data-team' }, removals: ['stale_property'] }
)
```

### Table Operations

#### `listTables(namespace, options?): Promise<{ identifiers, nextPageToken? }>`

List tables in a namespace, with cursor-based pagination.

```typescript
const { identifiers } = await catalog.listTables({ namespace: ['analytics'] })
// identifiers: [{ namespace: ['analytics'], name: 'events' }]

const page1 = await catalog.listTables({ namespace: ['analytics'] }, { pageSize: 100 })
const page2 = await catalog.listTables(
  { namespace: ['analytics'] },
  { pageSize: 100, pageToken: page1.nextPageToken }
)
```

#### `createTable(namespace: NamespaceIdentifier, request: CreateTableRequest): Promise<TableMetadata>`

Create a new table.

```typescript
const metadata = await catalog.createTable(
  { namespace: ['analytics'] },
  {
    name: 'events',
    schema: {
      type: 'struct',
      fields: [
        { id: 1, name: 'id', type: 'long', required: true },
        { id: 2, name: 'timestamp', type: 'timestamp', required: true },
      ],
      'schema-id': 0,
    },
    'partition-spec': {
      'spec-id': 0,
      fields: [
        {
          'source-id': 2,
          'field-id': 1000,
          name: 'ts_day',
          transform: 'day',
        },
      ],
    },
  }
)
```

#### `loadTable(id, options?): Promise<TableMetadata | null>`

Load table metadata. Pass `ifNoneMatch` (a previous ETag) for conditional GET — returns `null` on 304.

```typescript
const metadata = await catalog.loadTable({
  namespace: ['analytics'],
  name: 'events',
})

// Conditional load
const updated = await catalog.loadTable(
  { namespace: ['analytics'], name: 'events' },
  { ifNoneMatch: lastSeenEtag }
)
if (updated === null) {
  // table is unchanged since lastSeenEtag
}
```

#### `loadTableResult(id, options?): Promise<LoadTableResult & { etag } | null>`

Spec-aligned `LoadTableResult` wrapper exposing `metadata`, `metadata-location`, server `config`, `storage-credentials`, plus the captured `ETag` so you can pass it to a future `loadTable` call.

#### `updateTable(id, request): Promise<CommitTableResponse>` / `commitTable(id, request)`

Commit updates to a table using the spec-aligned `{ requirements?, updates }` shape.

```typescript
const updated = await catalog.updateTable(
  { namespace: ['analytics'], name: 'events' },
  {
    requirements: [{ type: 'assert-current-schema-id', 'current-schema-id': 0 }],
    updates: [{ action: 'set-properties', updates: { 'read.split.target-size': '134217728' } }],
  }
)
```

#### `dropTable(id: TableIdentifier): Promise<void>`

Drop a table from the catalog.

```typescript
await catalog.dropTable({ namespace: ['analytics'], name: 'events' })
```

## Error Handling

All API errors throw an `IcebergError` with details from the server:

```typescript
import { IcebergError } from 'iceberg-js'

try {
  await catalog.loadTable({ namespace: ['test'], name: 'missing' })
} catch (error) {
  if (error instanceof IcebergError) {
    console.log(error.status) // 404
    console.log(error.icebergType) // 'NoSuchTableException'
    console.log(error.message) // 'Table does not exist'
  }
}
```

## TypeScript Types

The library exports all relevant types:

```typescript
import type {
  // Identifiers
  NamespaceIdentifier,
  TableIdentifier,

  // Schema / type system
  TableSchema,
  TableField,
  IcebergType,
  PartitionSpec,
  SortOrder,

  // Requests / responses
  CreateTableRequest,
  CommitTableRequest,
  CommitTableResponse,
  LoadTableResult,
  LoadTableResultWithEtag,
  TableMetadata,
  UpdateNamespacePropertiesRequest,
  UpdateNamespacePropertiesResponse,

  // Method options
  ListNamespacesOptions,
  ListNamespacesResult,
  ListTablesOptions,
  ListTablesResult,
  LoadTableOptions,

  // Catalog config
  CatalogConfig,
  StorageCredential,

  // Table update / requirement unions (full spec coverage)
  TableUpdate,
  TableRequirement,

  // Auth / delegation
  AuthConfig,
  AccessDelegation,
} from 'iceberg-js'
```

## Supported Iceberg Types

The following Iceberg primitive types are supported:

- `boolean`, `int`, `long`, `float`, `double`
- `string`, `uuid`, `binary`
- `date`, `time`, `timestamp`, `timestamptz`
- `decimal(precision, scale)`, `fixed(length)`

## Compatibility

This package is built to work in **all** Node.js and JavaScript environments:

| Environment         | Module System        | Import Method                           | Status             |
| ------------------- | -------------------- | --------------------------------------- | ------------------ |
| Node.js ESM         | `"type": "module"`   | `import { ... } from 'iceberg-js'`      | Fully supported    |
| Node.js CommonJS    | Default              | `const { ... } = require('iceberg-js')` | Fully supported    |
| TypeScript ESM      | `module: "ESNext"`   | `import { ... } from 'iceberg-js'`      | Full type support  |
| TypeScript CommonJS | `module: "CommonJS"` | `import { ... } from 'iceberg-js'`      | Full type support  |
| Bundlers            | Any                  | Webpack, Vite, esbuild, Rollup, etc.    | Auto-detected      |
| Browsers            | ESM                  | `<script type="module">`                | Modern browsers    |
| Deno                | ESM                  | `import` from npm:                      | With npm specifier |

**Package exports:**

- ESM: `dist/index.mjs` with `dist/index.d.ts`
- CommonJS: `dist/index.cjs` with `dist/index.d.cts`
- Proper `exports` field for Node.js 12+ module resolution

All scenarios are tested in CI on Node.js 22.

## Browser Usage

The library works in modern browsers that support native `fetch`:

```typescript
import { IcebergRestCatalog } from 'iceberg-js'

const catalog = new IcebergRestCatalog({
  baseUrl: 'https://public-catalog.example.com/iceberg/v1',
  auth: { type: 'none' },
})

const namespaces = await catalog.listNamespaces()
```

## Custom fetch implementation

The library uses the global `fetch` by default (available in Node.js 22+ and modern browsers). You can inject a custom `fetch` for proxying, instrumentation, or to use a different HTTP client:

```typescript
import { IcebergRestCatalog } from 'iceberg-js'

const catalog = new IcebergRestCatalog({
  baseUrl: 'https://catalog.example.com/iceberg/v1',
  auth: { type: 'bearer', token: 'token' },
  fetch: myCustomFetch,
})
```

## Development

```bash
# Install dependencies
pnpm install

# Build the library
pnpm run build

# Run unit tests
pnpm test

# Run integration tests (requires Docker)
pnpm test:integration

# Run integration tests with cleanup (for CI)
pnpm test:integration:ci

# Run compatibility tests (all module systems)
pnpm test:compatibility

# Format code
pnpm run format

# Lint and test
pnpm run check
```

### Testing with Docker

Integration tests run against a local Iceberg REST Catalog in Docker. See [TESTING-DOCKER.md](./test/integration/TESTING-DOCKER.md) for details.

```bash
# Start Docker services and run integration tests
pnpm test:integration

# Or manually
docker compose up -d
npx tsx test/integration/test-local-catalog.ts
docker compose down -v
```

### Compatibility Testing

The `test:compatibility` script verifies the package works correctly in all JavaScript/TypeScript environments:

- **Pure JavaScript ESM** - Projects with `"type": "module"`
- **Pure JavaScript CommonJS** - Traditional Node.js projects
- **TypeScript ESM** - TypeScript with `module: "ESNext"`
- **TypeScript CommonJS** - TypeScript with `module: "CommonJS"`

These tests ensure proper module resolution, type definitions, and runtime behavior across all supported environments. See [test/compatibility/README.md](./test/compatibility/README.md) for more details.

## License

[MIT](./LICENSE)

## Releases

This project uses [release-please](https://github.com/googleapis/release-please) for automated releases. Here's how it works:

1. **Commit with conventional commits**: Use [Conventional Commits](https://www.conventionalcommits.org/) format for your commits:
   - `feat:` for new features (minor version bump)
   - `fix:` for bug fixes (patch version bump)
   - `feat!:` or `BREAKING CHANGE:` for breaking changes (major version bump)
   - `chore:`, `docs:`, `test:`, etc. for non-release commits

2. **Release PR is created automatically**: When you push to `main`, release-please creates/updates a release PR with:
   - Version bump in `package.json`
   - Updated `CHANGELOG.md`
   - Release notes

3. **Merge the release PR**: When you're ready to release, merge the PR. This will:
   - Create a GitHub release and git tag
   - Automatically publish to npm with provenance (using trusted publishing, no secrets needed)

**Example commits:**

```bash
git commit -m "feat: add support for view operations"
git commit -m "fix: handle empty namespace list correctly"
git commit -m "feat!: change auth config structure"
```

## Contributing

Contributions are welcome! Please ensure your contributions align with the library's [goals](#goals) and [non-goals](#non-goals). This library aims to remain a minimal, generic client for the Iceberg REST Catalog API.
