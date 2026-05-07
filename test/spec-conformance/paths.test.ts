import { describe, it, expect, beforeAll, vi } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'
import { loadSpec, matchSpecPath, type SpecBundle } from './load-spec'

interface CapturedRequest {
  method: string
  url: string
  headers: Record<string, string>
}

let spec: SpecBundle
let captured: CapturedRequest[]
let catalog: IcebergRestCatalog

beforeAll(async () => {
  spec = await loadSpec()
})

function makeFetch(): typeof fetch {
  captured = []
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString()
    const method = (init?.method ?? 'GET').toUpperCase()
    const headers: Record<string, string> = {}
    const h = init?.headers
    if (h) {
      if (h instanceof Headers) h.forEach((v, k) => (headers[k] = v))
      else if (Array.isArray(h)) for (const [k, v] of h) headers[k] = v
      else Object.assign(headers, h)
    }
    // Normalize header keys to lowercase so tests don't depend on what casing
    // the fetch path happened to preserve.
    const lower: Record<string, string> = {}
    for (const [k, v] of Object.entries(headers)) lower[k.toLowerCase()] = v
    captured.push({ method, url, headers: lower })

    // Plausible-shaped responses for every endpoint we exercise. The body shape
    // doesn't matter for path conformance — schemas.test.ts validates body shapes.
    if (url.endsWith('/v1/config') || url.includes('/v1/config?')) {
      return new Response(JSON.stringify({ defaults: {}, overrides: {} }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }
    if (method === 'HEAD') return new Response(null, { status: 200 })
    if (method === 'DELETE') return new Response(null, { status: 204 })
    if (url.includes('/namespaces/') && url.includes('/tables/')) {
      return new Response(
        JSON.stringify({
          metadata: {
            'format-version': 2,
            'table-uuid': '00000000-0000-0000-0000-000000000001',
            'current-schema-id': 0,
            schemas: [{ type: 'struct', fields: [], 'schema-id': 0 }],
            'partition-specs': [{ 'spec-id': 0, fields: [] }],
            'sort-orders': [{ 'order-id': 0, fields: [] }],
            properties: {},
          },
          'metadata-location': 's3://b/m.json',
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      )
    }
    if (url.endsWith('/tables') || url.includes('/tables?')) {
      return new Response(JSON.stringify({ identifiers: [] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }
    if (url.includes('/properties')) {
      return new Response(JSON.stringify({ updated: [], removed: [] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }
    return new Response(JSON.stringify({ namespaces: [] }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    })
  }) as unknown as typeof fetch
}

function newCatalog(opts?: { warehouse?: string }): IcebergRestCatalog {
  return new IcebergRestCatalog({
    baseUrl: 'https://example.com',
    fetch: makeFetch(),
    ...opts,
  })
}

describe('Spec conformance — paths and methods', () => {
  it('every emitted (METHOD, path) matches a spec operation (no warehouse)', async () => {
    catalog = newCatalog()
    await catalog.listNamespaces()
    await catalog.listNamespaces({ parent: { namespace: ['analytics'] }, pageSize: 50 })
    await catalog.createNamespace({ namespace: ['analytics'] }, { properties: { owner: 'me' } })
    await catalog.dropNamespace({ namespace: ['analytics'] })
    await catalog.loadNamespaceMetadata({ namespace: ['analytics'] })
    await catalog.namespaceExists({ namespace: ['analytics'] })
    await catalog.updateNamespaceProperties({ namespace: ['analytics'] }, { updates: { x: 'y' } })
    await catalog.listTables({ namespace: ['analytics'] }, { pageSize: 10 })
    await catalog.createTable(
      { namespace: ['analytics'] },
      {
        name: 'events',
        schema: { type: 'struct', fields: [], 'schema-id': 0 },
      }
    )
    await catalog.loadTable({ namespace: ['analytics'], name: 'events' })
    await catalog.tableExists({ namespace: ['analytics'], name: 'events' })
    await catalog.updateTable(
      { namespace: ['analytics'], name: 'events' },
      { updates: [{ action: 'set-properties', updates: { a: 'b' } }] }
    )
    await catalog.dropTable({ namespace: ['analytics'], name: 'events' })

    expect(captured.length).toBeGreaterThan(0)

    const specPaths = Array.from(spec.pathMethods.keys())
    const unmatched: string[] = []

    for (const req of captured) {
      const u = new URL(req.url)
      const matched = matchSpecPath(u.pathname, specPaths)
      if (!matched) {
        unmatched.push(`${req.method} ${u.pathname}`)
        continue
      }
      const allowed = spec.pathMethods.get(matched)!
      if (!allowed.has(req.method)) {
        unmatched.push(
          `${req.method} ${u.pathname} (matched ${matched}; spec allows ${[...allowed].join(',')})`
        )
      }
    }

    expect(unmatched).toEqual([])
  })

  it('every mutation request carries an Idempotency-Key', async () => {
    catalog = newCatalog()
    await catalog.createNamespace({ namespace: ['x'] })
    await catalog.dropNamespace({ namespace: ['x'] })
    await catalog.createTable(
      { namespace: ['x'] },
      { name: 't', schema: { type: 'struct', fields: [], 'schema-id': 0 } }
    )
    await catalog.updateTable({ namespace: ['x'], name: 't' }, { updates: [] })
    await catalog.dropTable({ namespace: ['x'], name: 't' })
    await catalog.updateNamespaceProperties({ namespace: ['x'] }, { updates: { k: 'v' } })

    const mutations = captured.filter((r) => ['POST', 'DELETE'].includes(r.method))
    expect(mutations.length).toBeGreaterThan(0)
    for (const req of mutations) {
      expect(req.headers['idempotency-key']).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
      )
    }
  })

  it('warehouse path goes through /v1/config first and uses the returned prefix', async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.includes('/v1/config')) {
        return new Response(JSON.stringify({ defaults: {}, overrides: { prefix: 'srv-prefix' } }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        })
      }
      return new Response(JSON.stringify({ namespaces: [] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }) as unknown as typeof fetch

    const catalog = new IcebergRestCatalog({
      baseUrl: 'https://example.com',
      warehouse: 'wh',
      fetch: fetchImpl,
    })
    await catalog.listNamespaces()

    const calls = (fetchImpl as unknown as { mock: { calls: [URL | string][] } }).mock.calls
    expect(String(calls[0][0])).toBe('https://example.com/v1/config?warehouse=wh')
    expect(String(calls[1][0])).toBe('https://example.com/v1/srv-prefix/namespaces')

    // The path with the resolved prefix must still match the spec template.
    const u = new URL(String(calls[1][0]))
    const specPaths = Array.from(spec.pathMethods.keys())
    expect(matchSpecPath(u.pathname, specPaths)).toBe('/v1/{prefix}/namespaces')
  })

  it('the v1/config endpoint is itself a spec operation', () => {
    expect(spec.operations.has('GET /v1/config')).toBe(true)
  })
})
