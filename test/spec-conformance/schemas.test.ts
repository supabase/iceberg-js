import { describe, it, expect, beforeAll, vi } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'
import { loadSpec, type SpecBundle } from './load-spec'

let spec: SpecBundle

beforeAll(async () => {
  spec = await loadSpec()
})

interface CapturedRequest {
  method: string
  url: string
  body?: unknown
}

function makeFetch(): { fetch: typeof fetch; captured: CapturedRequest[] } {
  const captured: CapturedRequest[] = []
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString()
    const method = (init?.method ?? 'GET').toUpperCase()
    let body: unknown
    if (typeof init?.body === 'string') {
      try {
        body = JSON.parse(init.body)
      } catch {
        body = init.body
      }
    }
    captured.push({ method, url, body })

    return new Response(
      JSON.stringify({
        // Just enough to make every method's response parser happy. Each test
        // overrides as needed.
        namespaces: [],
        identifiers: [],
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
        updated: [],
        removed: [],
        defaults: {},
        overrides: {},
      }),
      { status: 200, headers: { 'content-type': 'application/json' } }
    )
  }) as unknown as typeof fetch
  return { fetch: fn, captured }
}

function newCatalog(): { catalog: IcebergRestCatalog; captured: CapturedRequest[] } {
  const { fetch, captured } = makeFetch()
  return {
    catalog: new IcebergRestCatalog({ baseUrl: 'https://example.com', fetch }),
    captured,
  }
}

function validate(schemaName: string, value: unknown): void {
  const validator = spec.validators.get(schemaName)
  expect(validator, `schema "${schemaName}" should be loaded`).toBeDefined()
  const ok = validator!(value)
  if (!ok) {
    const errs = (validator!.errors ?? []).map(
      (e) => `  ${e.instancePath || '/'} ${e.message ?? '(no message)'} ${JSON.stringify(e.params)}`
    )
    throw new Error(
      `Body failed ${schemaName} validation:\n${errs.join('\n')}\nBody: ${JSON.stringify(value, null, 2)}`
    )
  }
}

describe('Spec conformance — request bodies', () => {
  it('createNamespace body conforms to CreateNamespaceRequest', async () => {
    const { catalog, captured } = newCatalog()
    await catalog.createNamespace(
      { namespace: ['analytics'] },
      { properties: { owner: 'data-team' } }
    )
    const req = captured.find((r) => r.method === 'POST')!
    validate('CreateNamespaceRequest', req.body)
  })

  it('updateNamespaceProperties body conforms to UpdateNamespacePropertiesRequest', async () => {
    const { catalog, captured } = newCatalog()
    await catalog.updateNamespaceProperties(
      { namespace: ['analytics'] },
      { updates: { x: 'y' }, removals: ['old'] }
    )
    const req = captured.find((r) => r.url.includes('/properties'))!
    validate('UpdateNamespacePropertiesRequest', req.body)
  })

  it('createTable body conforms to CreateTableRequest', async () => {
    const { catalog, captured } = newCatalog()
    await catalog.createTable(
      { namespace: ['analytics'] },
      {
        name: 'events',
        schema: {
          type: 'struct',
          fields: [
            { id: 1, name: 'id', type: 'long', required: true },
            { id: 2, name: 'ts', type: 'timestamp', required: true },
          ],
          'schema-id': 0,
        },
        'partition-spec': { 'spec-id': 0, fields: [] },
        'write-order': { 'order-id': 0, fields: [] },
        properties: { 'write.format.default': 'parquet' },
      }
    )
    const req = captured.find((r) => r.method === 'POST')!
    validate('CreateTableRequest', req.body)
  })

  it('updateTable body conforms to CommitTableRequest', async () => {
    const { catalog, captured } = newCatalog()
    await catalog.updateTable(
      { namespace: ['analytics'], name: 'events' },
      {
        requirements: [{ type: 'assert-table-uuid', uuid: '00000000-0000-0000-0000-000000000001' }],
        updates: [
          { action: 'set-properties', updates: { 'read.split.target-size': '134217728' } },
          { action: 'remove-properties', removals: ['stale'] },
        ],
      }
    )
    const req = captured.find((r) => r.url.endsWith('/events'))!
    validate('CommitTableRequest', req.body)
  })
})

describe('Spec conformance — response shapes (server fixtures)', () => {
  it('IcebergErrorResponse fixture validates', () => {
    validate('IcebergErrorResponse', {
      error: { message: 'no', type: 'NoSuchTableException', code: 404 },
    })
  })

  it('CatalogConfig fixture validates', () => {
    validate('CatalogConfig', {
      defaults: { clients: '4' },
      overrides: { prefix: 'srv' },
      endpoints: ['GET /v1/{prefix}/namespaces'],
      'idempotency-key-lifetime': 'PT30M',
    })
  })

  it('ListNamespacesResponse fixture validates', () => {
    validate('ListNamespacesResponse', {
      namespaces: [['default'], ['analytics']],
      'next-page-token': 'tok',
    })
  })

  it('ListTablesResponse fixture validates', () => {
    validate('ListTablesResponse', {
      identifiers: [{ namespace: ['analytics'], name: 'events' }],
    })
  })

  it('CreateNamespaceResponse fixture validates', () => {
    validate('CreateNamespaceResponse', {
      namespace: ['analytics'],
      properties: { owner: 'data-team' },
    })
  })

  it('GetNamespaceResponse fixture validates', () => {
    validate('GetNamespaceResponse', {
      namespace: ['analytics'],
      properties: { owner: 'data-team' },
    })
  })
})
