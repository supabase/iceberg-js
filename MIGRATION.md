# Migrating from 0.x to 1.0

The `iceberg-js` 1.0 release realigns the client with the [Apache Iceberg REST Catalog OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml). All breaking changes are listed below with before / after code blocks. The shape changes are mechanical — most upgrades are 5–20 lines of code.

> If you only want the cheat sheet, jump to [Quick reference](#quick-reference) at the bottom.

## Contents

1. [`listNamespaces()` return shape](#listnamespaces-return-shape)
2. [`listNamespaces` parent argument](#listnamespaces-parent-argument)
3. [`listTables()` return shape](#listtables-return-shape)
4. [`updateTable()` request body shape](#updatetable-request-body-shape)
5. [`catalogName` → `warehouse`](#catalogname--warehouse)
6. [`accessDelegation`: use `loadTableResult` / `createTableResult`](#accessdelegation-use-loadtableresult--createtableresult)
7. [Node 18 / 20 dropped](#node-18--20-dropped)
8. [Non-breaking additions](#non-breaking-additions)
9. [Quick reference](#quick-reference)

---

## `listNamespaces()` return shape

The catalog now returns a paginated response per the spec.

```ts
// 0.x
const namespaces = await catalog.listNamespaces()
for (const ns of namespaces) {
  console.log(ns.namespace)
}
```

```ts
// 1.0
const { namespaces, nextPageToken } = await catalog.listNamespaces()
for (const ns of namespaces) {
  console.log(ns.namespace)
}
```

If you need to iterate every namespace across pages:

```ts
let pageToken: string | undefined
do {
  const page = await catalog.listNamespaces({ pageSize: 100, pageToken })
  for (const ns of page.namespaces) {
    // ...
  }
  pageToken = page.nextPageToken
} while (pageToken)
```

## `listNamespaces` parent argument

The "list children under parent X" form moved into a named `parent` option to make room for pagination params.

```ts
// 0.x
const children = await catalog.listNamespaces({ namespace: ['analytics'] })
```

```ts
// 1.0
const { namespaces: children } = await catalog.listNamespaces({
  parent: { namespace: ['analytics'] },
})
```

## `listTables()` return shape

Same pagination treatment as `listNamespaces`, but the field is named `identifiers` per the spec.

```ts
// 0.x
const tables = await catalog.listTables({ namespace: ['analytics'] })
for (const t of tables) {
  console.log(t.name)
}
```

```ts
// 1.0
const { identifiers: tables, nextPageToken } = await catalog.listTables({
  namespace: ['analytics'],
})
for (const t of tables) {
  console.log(t.name)
}
```

## `updateTable()` request body shape

`updateTable` (and its alias `commitTable`) now takes the spec-shaped `{ requirements, updates }` body. The most common case — setting properties — looks like this:

```ts
// 0.x
await catalog.updateTable(
  { namespace: ['analytics'], name: 'events' },
  { properties: { 'read.split.target-size': '134217728' } }
)
```

```ts
// 1.0
await catalog.updateTable(
  { namespace: ['analytics'], name: 'events' },
  {
    requirements: [],
    updates: [
      {
        action: 'set-properties',
        updates: { 'read.split.target-size': '134217728' },
      },
    ],
  }
)
```

Removing properties:

```ts
// 1.0
await catalog.updateTable(
  { namespace: ['analytics'], name: 'events' },
  {
    requirements: [],
    updates: [
      {
        action: 'remove-properties',
        removals: ['stale-property'],
      },
    ],
  }
)
```

The full `TableUpdate` and `TableRequirement` discriminated unions are exported — see [`src/catalog/types.ts`](./src/catalog/types.ts) for the complete surface (schema changes, partition spec changes, snapshot management, etc.). `requirements: []` opts out of optimistic concurrency; pass `AssertTableUUID`, `AssertRefSnapshotId`, etc. when you want commit-time checks.

## `catalogName` → `warehouse`

`catalogName` used to be appended to the URL path manually (`/v1/<catalogName>/...`). 1.0 follows the spec's recommended pattern: pass `warehouse` and the client calls `GET /v1/config?warehouse=…` on first use, then uses the server-returned `overrides.prefix` for all subsequent requests.

```ts
// 0.x
const catalog = new IcebergRestCatalog({
  baseUrl: 'https://catalog.example.com',
  catalogName: 'my-warehouse',
  auth: { type: 'bearer', token: process.env.ICEBERG_TOKEN },
})
```

```ts
// 1.0
const catalog = new IcebergRestCatalog({
  baseUrl: 'https://catalog.example.com',
  warehouse: 'my-warehouse',
  auth: { type: 'bearer', token: process.env.ICEBERG_TOKEN },
})
```

`catalogName` is kept as an alias for backward compatibility — existing code keeps working. If both are set, `warehouse` wins.

If the server does not implement `/v1/config`, the client falls back to using the warehouse as a literal path segment (matching the legacy `catalogName` behavior), so the upgrade is safe against older deployments too.

## `accessDelegation`: use `loadTableResult` / `createTableResult`

This is the easiest footgun to miss. In 0.x, `loadTable` returned credentials inline in the metadata. In 1.0, `loadTable` returns the bare `TableMetadata` per the spec — server-vended credentials and config are only accessible through the new `*Result` methods.

```ts
const catalog = new IcebergRestCatalog({
  baseUrl: 'https://catalog.example.com',
  warehouse: 'my-warehouse',
  auth: { type: 'bearer', token: process.env.ICEBERG_TOKEN },
  accessDelegation: ['vended-credentials'],
})
```

```ts
// 0.x — credentials were folded into the metadata response
const metadata = await catalog.loadTable({ namespace: ['analytics'], name: 'events' })
// metadata.config had the credentials
```

```ts
// 1.0 — use loadTableResult to receive storage-credentials, config, and the ETag
const result = await catalog.loadTableResult({
  namespace: ['analytics'],
  name: 'events',
})
// result.metadata, result.config, result['storage-credentials'], result.etag
```

The same applies to creating / registering tables: `createTableResult(...)` and `registerTableResult(...)` return the full spec-shaped `LoadTableResult` plus the captured ETag.

If you are _not_ using `accessDelegation`, you do not need these methods — `loadTable`, `createTable`, and `registerTable` keep working and return `TableMetadata` directly.

## Node 18 / 20 dropped

1.0 requires **Node 22+**. Node 18 reached end-of-life on 2025-04-30; Node 20 reaches EOL on 2026-04-30. If you are still on those runtimes:

- Stay on `iceberg-js@^0.8.x` for the time being (it supports Node 18+).
- Or upgrade your runtime. Most projects already track Node 22 LTS.

## Non-breaking additions

You can ignore these on upgrade — they're listed here so you know they exist.

- **`Idempotency-Key`** is automatically emitted on every POST / DELETE mutation. No code change needed; retried writes are now safe by default.
- **Conditional `loadTable`**: `loadTable(id, { ifNoneMatch })` performs a conditional GET. Returns `null` on 304 Not Modified.
- **Snapshot selection**: `loadTable(id, { snapshots: 'all' | 'refs' })` controls how many snapshots the server includes.
- **Spec-shaped result variants**: `loadTableResult`, `createTableResult`, `registerTableResult` return the full `LoadTableResult` (metadata + config + storage-credentials + etag).
- **Catalog operations now exposed**: `registerTable(namespace, request)`, `renameTable(request)`, `updateNamespaceProperties(id, request)`, `commitTable(id, request)` (alias for `updateTable`), `loadConfig()`.
- **Network errors normalized**: DNS / TLS / offline failures now surface as `IcebergError` with `status: 0`. A single `instanceof IcebergError` check now catches every failure mode.

## Quick reference

| 0.x                                                              | 1.0                                                                                 |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `listNamespaces()` → `NamespaceIdentifier[]`                     | `listNamespaces()` → `{ namespaces, nextPageToken? }`                               |
| `listNamespaces({ namespace })`                                  | `listNamespaces({ parent })`                                                        |
| `listTables(ns)` → `TableIdentifier[]`                           | `listTables(ns)` → `{ identifiers, nextPageToken? }`                                |
| `updateTable({ properties })`                                    | `updateTable({ requirements, updates: [{ action: 'set-properties', updates }] })`   |
| `catalogName: 'x'`                                               | `warehouse: 'x'` (alias `catalogName` still works)                                  |
| `loadTable(id)` returned credentials inline (`accessDelegation`) | `loadTableResult(id)` for credentials; `loadTable(id)` returns bare `TableMetadata` |
| Node 18+                                                         | Node 22+                                                                            |

If you hit a case not covered here, please file an issue — the API surface is meant to be a thin, faithful wrapper over the [Iceberg REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
