import { describe, it, expect, beforeAll, vi } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'
import { IcebergError } from '../../src/errors/IcebergError'
import { loadSpec, type SpecBundle } from './load-spec'
import { createSpecValidatingFetch, SpecValidationError } from './spec-validating-fetch'

let spec: SpecBundle
beforeAll(async () => {
  spec = await loadSpec()
})

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  })
}

const TABLE_METADATA = {
  'format-version': 2,
  'table-uuid': '00000000-0000-0000-0000-000000000001',
  'current-schema-id': 0,
  schemas: [{ type: 'struct', fields: [], 'schema-id': 0 }],
  'partition-specs': [{ 'spec-id': 0, fields: [] }],
  'sort-orders': [{ 'order-id': 0, fields: [] }],
  properties: {},
}

describe('regressions — driven by spec, not by reviewer', () => {
  describe('B1: path-segment encoding', () => {
    it('encodes a "/" inside a table name as %2F (drives B1)', async () => {
      const seen: string[] = []
      const realFetch = vi.fn(async (input: RequestInfo | URL) => {
        seen.push(String(input))
        return jsonResponse(200, { metadata: TABLE_METADATA, 'metadata-location': 's3://b/m' })
      }) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        fetch: realFetch,
      })

      await catalog.loadTable({ namespace: ['analytics'], name: 'foo/bar' })

      // The single logical table-name segment is "foo/bar" — it must be
      // percent-encoded, not turned into two path segments.
      const u = new URL(seen[0])
      const segs = u.pathname.split('/').filter(Boolean)
      expect(segs).toEqual(['v1', 'namespaces', 'analytics', 'tables', 'foo%2Fbar'])
    })

    it('encodes a "%" inside a namespace part as %25 (drives B1)', async () => {
      const seen: string[] = []
      const realFetch = vi.fn(async (input: RequestInfo | URL) => {
        seen.push(String(input))
        return jsonResponse(200, { metadata: TABLE_METADATA, 'metadata-location': 's3://b/m' })
      }) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        fetch: realFetch,
      })

      await catalog.loadTable({ namespace: ['100%team', 'sub'], name: 'events' })

      const u = new URL(seen[0])
      // %25 (the `%`) followed by 1F (literal hex 1F) must remain distinct
      // from the namespace separator `%1F`. Decoded segment must be
      // "100%team\x1Fsub", not "100%1Fteam\x1Fsub" (which would happen if we
      // forget to encode `%`).
      expect(u.pathname).toContain('100%25team%1Fsub')
    })

    it('passes spec-validating fetch with a normal table name', async () => {
      const realFetch = vi.fn(async () =>
        jsonResponse(200, { metadata: TABLE_METADATA, 'metadata-location': 's3://b/m' })
      ) as unknown as typeof fetch
      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        fetch: createSpecValidatingFetch(spec, { realFetch }),
      })
      await expect(
        catalog.loadTable({ namespace: ['analytics'], name: 'events' })
      ).resolves.toBeDefined()
    })
  })

  describe('B2: createTableResult exposes storage-credentials', () => {
    it('returns the full LoadTableResult shape (drives B2)', async () => {
      const realFetch = vi.fn(async () =>
        jsonResponse(200, {
          metadata: TABLE_METADATA,
          'metadata-location': 's3://b/m',
          config: { 'token.refresh.enabled': 'true' },
          'storage-credentials': [{ prefix: 's3://bucket/', config: { 'access-key-id': 'AKIA…' } }],
        })
      ) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        accessDelegation: ['vended-credentials'],
        fetch: createSpecValidatingFetch(spec, { realFetch }),
      })

      const result = await catalog.createTableResult(
        { namespace: ['analytics'] },
        { name: 'events', schema: { type: 'struct', fields: [], 'schema-id': 0 } }
      )

      expect(result).not.toBeNull()
      expect(result!['storage-credentials']).toEqual([
        { prefix: 's3://bucket/', config: { 'access-key-id': 'AKIA…' } },
      ])
      expect(result!.config).toEqual({ 'token.refresh.enabled': 'true' })
      expect(result!['metadata-location']).toBe('s3://b/m')
    })
  })

  describe('B3: updateTable rejects malformed responses', () => {
    it('throws when metadata-location is missing on 200 (drives B3)', async () => {
      const realFetch = vi.fn(async () =>
        // Spec requires metadata-location on CommitTableResponse — server is
        // misbehaving. The client must not silently substitute "".
        jsonResponse(200, { metadata: TABLE_METADATA })
      ) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({ baseUrl: 'https://example.com', fetch: realFetch })

      await expect(
        catalog.updateTable({ namespace: ['x'], name: 't' }, { requirements: [], updates: [] })
      ).rejects.toThrow(IcebergError)
    })
  })

  describe('B4: loadConfig race-free', () => {
    it('three concurrent first calls produce one fetch (drives B4)', async () => {
      let configCalls = 0
      const realFetch = vi.fn(async (input: RequestInfo | URL) => {
        const u = String(input)
        if (u.includes('/v1/config')) {
          configCalls++
          // Slight delay so all three callers see the in-flight promise.
          await new Promise((r) => setTimeout(r, 10))
          return jsonResponse(200, { defaults: {}, overrides: { prefix: 'wh' } })
        }
        return jsonResponse(200, { namespaces: [] })
      }) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        warehouse: 'wh',
        fetch: realFetch,
      })

      await Promise.all([catalog.loadConfig(), catalog.loadConfig(), catalog.loadConfig()])

      expect(configCalls).toBe(1)
    })
  })

  describe('B5: IcebergError.isCommitStateUnknown is exact', () => {
    it('does not false-positive on unrelated 500 types (drives B5)', () => {
      const err = new IcebergError('boom', {
        status: 500,
        icebergType: 'CommitStateNotKnownButCloseEnoughException',
      })
      expect(err.isCommitStateUnknown).toBe(false)
    })

    it('matches the exact spec-defined type', () => {
      const err = new IcebergError('boom', {
        status: 500,
        icebergType: 'CommitStateUnknownException',
      })
      expect(err.isCommitStateUnknown).toBe(true)
    })
  })

  describe('B6: network errors wrap as IcebergError', () => {
    it('a fetch that throws TypeError surfaces as IcebergError (drives B6)', async () => {
      const realFetch = vi.fn(async () => {
        throw new TypeError('Failed to fetch')
      }) as unknown as typeof fetch

      const catalog = new IcebergRestCatalog({ baseUrl: 'https://example.com', fetch: realFetch })

      await expect(catalog.listNamespaces()).rejects.toBeInstanceOf(IcebergError)
    })
  })

  describe('A4: matchSpecPath warehouse-prefix tightening', () => {
    it('rejects an unprefixed call when warehouse is configured', async () => {
      const buggyClient = vi.fn(async (input: RequestInfo | URL) => {
        const u = String(input)
        if (u.includes('/v1/config')) {
          return jsonResponse(200, { defaults: {}, overrides: { prefix: 'wh' } })
        }
        return jsonResponse(200, { namespaces: [] })
      }) as unknown as typeof fetch

      // Construct a wrapper that pretends the warehouse is configured. Then
      // synthesize a call with the prefix dropped and confirm the wrapper
      // rejects it. (We can't easily make IcebergRestCatalog drop the prefix
      // without touching production code, so we drive the wrapper directly.)
      const validating = createSpecValidatingFetch(spec, {
        realFetch: buggyClient,
        warehouseConfigured: true,
      })

      await expect(validating('https://example.com/v1/namespaces')).rejects.toBeInstanceOf(
        SpecValidationError
      )
    })
  })

  describe('Spec-validating fetch sanity', () => {
    it('catches Idempotency-Key absence on createNamespace', async () => {
      // Bypass the production code so we can simulate a buggy client that
      // omits the header. Drive the wrapper directly.
      const validating = createSpecValidatingFetch(spec, {
        realFetch: async () => jsonResponse(200, { namespace: ['x'] }),
      })
      await expect(
        validating('https://example.com/v1/namespaces', {
          method: 'POST',
          body: JSON.stringify({ namespace: ['x'] }),
          headers: { 'content-type': 'application/json' },
        })
      ).rejects.toBeInstanceOf(SpecValidationError)
    })

    it('catches a non-UUIDv7 Idempotency-Key', async () => {
      const validating = createSpecValidatingFetch(spec, {
        realFetch: async () => jsonResponse(200, { namespace: ['x'] }),
      })
      await expect(
        validating('https://example.com/v1/namespaces', {
          method: 'POST',
          body: JSON.stringify({ namespace: ['x'] }),
          headers: {
            'content-type': 'application/json',
            'idempotency-key': '00000000-0000-4000-8000-000000000000', // v4
          },
        })
      ).rejects.toBeInstanceOf(SpecValidationError)
    })

    it('catches a request body that does not match the spec', async () => {
      const validating = createSpecValidatingFetch(spec, {
        realFetch: async () => jsonResponse(200, { namespace: ['x'] }),
      })
      await expect(
        validating('https://example.com/v1/namespaces', {
          method: 'POST',
          body: JSON.stringify({ wrong: 'shape' }),
          headers: {
            'content-type': 'application/json',
            'idempotency-key': '01890c8a-1f6e-7af2-b1c8-9a8765432100',
          },
        })
      ).rejects.toBeInstanceOf(SpecValidationError)
    })

    it('catches a response body that does not match the spec', async () => {
      const validating = createSpecValidatingFetch(spec, {
        realFetch: async () =>
          // Spec says CreateNamespaceResponse requires a `namespace` field.
          jsonResponse(200, { properties: { x: 'y' } }),
      })
      await expect(
        validating('https://example.com/v1/namespaces', {
          method: 'POST',
          body: JSON.stringify({ namespace: ['x'] }),
          headers: {
            'content-type': 'application/json',
            'idempotency-key': '01890c8a-1f6e-7af2-b1c8-9a8765432100',
          },
        })
      ).rejects.toBeInstanceOf(SpecValidationError)
    })
  })

  describe('Full-method spec sweep through validating fetch', () => {
    // End-to-end check: every public method on IcebergRestCatalog goes through
    // the spec-validating fetch wrapper. If any method emits a request the
    // pinned spec rejects (path, method, body, idempotency, response shape),
    // these tests fail — without anyone having to read the diff.
    function makeRealFetch() {
      return async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input)
        const method = (init?.method ?? 'GET').toUpperCase()
        if (url.includes('/v1/config')) {
          return jsonResponse(200, { defaults: {}, overrides: {} })
        }
        if (url.endsWith('/properties')) {
          return jsonResponse(200, { updated: ['k'], removed: [] })
        }
        if (url.endsWith('/tables/rename')) {
          return new Response(null, { status: 204 })
        }
        if (url.endsWith('/register')) {
          return jsonResponse(200, {
            metadata: TABLE_METADATA,
            'metadata-location': 's3://b/m.json',
          })
        }
        if (url.includes('/namespaces/') && url.includes('/tables/')) {
          if (method === 'POST') {
            // commit/update returns CommitTableResponse
            return jsonResponse(200, {
              metadata: TABLE_METADATA,
              'metadata-location': 's3://b/m.json',
            })
          }
          return jsonResponse(200, {
            metadata: TABLE_METADATA,
            'metadata-location': 's3://b/m.json',
          })
        }
        if (url.endsWith('/tables') || url.includes('/tables?')) {
          if (method === 'POST') {
            // createTable returns LoadTableResult
            return jsonResponse(200, {
              metadata: TABLE_METADATA,
              'metadata-location': 's3://b/m.json',
            })
          }
          return jsonResponse(200, { identifiers: [] })
        }
        // /v1/namespaces/{ns} — single namespace
        if (url.match(/\/v1\/namespaces\/[^/]+(?:\?|$)/)) {
          return jsonResponse(200, { namespace: ['analytics'], properties: {} })
        }
        // /v1/namespaces — list or create
        if (url.match(/\/v1\/namespaces(?:\?|$)/)) {
          if (method === 'POST') {
            return jsonResponse(200, { namespace: ['analytics'], properties: {} })
          }
          return jsonResponse(200, { namespaces: [['analytics']] })
        }
        return jsonResponse(200, {})
      }
    }

    function newCatalog() {
      return new IcebergRestCatalog({
        baseUrl: 'https://example.com',
        fetch: createSpecValidatingFetch(spec, { realFetch: makeRealFetch() }),
      })
    }

    it('listNamespaces (no parent, paginated)', async () => {
      await newCatalog().listNamespaces({ pageSize: 10 })
    })
    it('listNamespaces (with parent)', async () => {
      await newCatalog().listNamespaces({ parent: { namespace: ['analytics'] } })
    })
    it('createNamespace', async () => {
      await newCatalog().createNamespace({ namespace: ['analytics'] }, { properties: { o: 'me' } })
    })
    it('dropNamespace', async () => {
      await newCatalog().dropNamespace({ namespace: ['analytics'] })
    })
    it('loadNamespaceMetadata', async () => {
      await newCatalog().loadNamespaceMetadata({ namespace: ['analytics'] })
    })
    it('namespaceExists', async () => {
      await newCatalog().namespaceExists({ namespace: ['analytics'] })
    })
    it('updateNamespaceProperties', async () => {
      await newCatalog().updateNamespaceProperties(
        { namespace: ['analytics'] },
        { updates: { k: 'v' }, removals: ['old'] }
      )
    })
    it('listTables', async () => {
      await newCatalog().listTables({ namespace: ['analytics'] })
    })
    it('createTable', async () => {
      await newCatalog().createTable(
        { namespace: ['analytics'] },
        { name: 'events', schema: { type: 'struct', fields: [], 'schema-id': 0 } }
      )
    })
    it('createTableResult', async () => {
      await newCatalog().createTableResult(
        { namespace: ['analytics'] },
        { name: 'events', schema: { type: 'struct', fields: [], 'schema-id': 0 } }
      )
    })
    it('loadTable', async () => {
      await newCatalog().loadTable({ namespace: ['analytics'], name: 'events' })
    })
    it('loadTable with snapshots query', async () => {
      await newCatalog().loadTable(
        { namespace: ['analytics'], name: 'events' },
        { snapshots: 'refs' }
      )
    })
    it('loadTableResult', async () => {
      await newCatalog().loadTableResult({ namespace: ['analytics'], name: 'events' })
    })
    it('tableExists', async () => {
      await newCatalog().tableExists({ namespace: ['analytics'], name: 'events' })
    })
    it('updateTable', async () => {
      await newCatalog().updateTable(
        { namespace: ['analytics'], name: 'events' },
        { requirements: [{ type: 'assert-create' }], updates: [] }
      )
    })
    it('commitTable', async () => {
      await newCatalog().commitTable(
        { namespace: ['analytics'], name: 'events' },
        { requirements: [], updates: [{ action: 'set-properties', updates: { k: 'v' } }] }
      )
    })
    it('dropTable', async () => {
      await newCatalog().dropTable({ namespace: ['analytics'], name: 'events' })
    })
    it('registerTable', async () => {
      await newCatalog().registerTable(
        { namespace: ['analytics'] },
        { name: 'r', 'metadata-location': 's3://b/m.json' }
      )
    })
    it('renameTable', async () => {
      await newCatalog().renameTable({
        source: { namespace: ['analytics'], name: 'events' },
        destination: { namespace: ['analytics'], name: 'events_v2' },
      })
    })
  })
})
