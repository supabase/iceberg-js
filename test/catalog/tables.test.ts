import { describe, it, expect, vi } from 'vitest'
import { TableOperations } from '../../src/catalog/tables'
import { IcebergError } from '../../src/errors/IcebergError'
import type { HttpClient } from '../../src/http/types'

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

const createMockClient = (): HttpClient => ({
  request: vi.fn(),
})

const mockTableMetadata = {
  'format-version': 2,
  'table-uuid': '00000000-0000-0000-0000-000000000001',
  name: 'events',
  location: 's3://bucket/warehouse/analytics/events',
  'current-schema-id': 0,
  schemas: [
    {
      type: 'struct' as const,
      fields: [
        { id: 1, name: 'id', type: 'long', required: true },
        { id: 2, name: 'timestamp', type: 'timestamp', required: true },
      ],
      'schema-id': 0,
    },
  ],
  'partition-specs': [{ 'spec-id': 0, fields: [] }],
  'sort-orders': [{ 'order-id': 0, fields: [] }],
  properties: {},
  'metadata-location': 's3://bucket/warehouse/analytics/events/metadata/v1.json',
}

describe('TableOperations', () => {
  describe('listTables', () => {
    it('should list tables in a namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          identifiers: [
            { namespace: ['analytics'], name: 'events' },
            { namespace: ['analytics'], name: 'users' },
          ],
        },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.listTables({ namespace: ['analytics'] })

      expect(result).toEqual({
        identifiers: [
          { namespace: ['analytics'], name: 'events' },
          { namespace: ['analytics'], name: 'users' },
        ],
        nextPageToken: undefined,
      })
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables',
        query: undefined,
      })
    })

    it('should expose nextPageToken from the response', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { identifiers: [], 'next-page-token': 'abc' },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.listTables({ namespace: ['analytics'] }, { pageSize: 50 })

      expect(result.nextPageToken).toBe('abc')
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables',
        query: { pageSize: '50' },
      })
    })

    it('should list tables in multipart namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { identifiers: [] },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.listTables({ namespace: ['analytics', 'prod'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics%1Fprod/tables',
        query: undefined,
      })
    })
  })

  describe('createTable', () => {
    it('should create a table and emit Idempotency-Key', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.createTable(
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
          'partition-spec': { 'spec-id': 0, fields: [] },
        }
      )

      expect(result).toEqual(mockTableMetadata)
      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.method).toBe('POST')
      expect(callArg.path).toBe('/v1/namespaces/analytics/tables')
      expect(callArg.body).toMatchObject({ name: 'events' })
      expect(callArg.headers?.['Idempotency-Key']).toMatch(UUID_RE)
    })

    it('should create table with partition spec', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.createTable(
        { namespace: ['analytics'] },
        {
          name: 'events',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', type: 'long', required: true }],
            'schema-id': 0,
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [{ 'source-id': 2, 'field-id': 1000, name: 'ts_day', transform: 'day' }],
          },
        }
      )

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.body).toMatchObject({
        'partition-spec': {
          'spec-id': 0,
          fields: [{ 'source-id': 2, 'field-id': 1000, name: 'ts_day', transform: 'day' }],
        },
      })
    })

    it('should create table with properties', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.createTable(
        { namespace: ['analytics'] },
        {
          name: 'events',
          schema: { type: 'struct', fields: [], 'schema-id': 0 },
          properties: {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'snappy',
          },
        }
      )

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.body).toMatchObject({
        properties: {
          'write.format.default': 'parquet',
          'write.parquet.compression-codec': 'snappy',
        },
      })
    })
  })

  describe('loadTable', () => {
    it('should load a table', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.loadTable({ namespace: ['analytics'], name: 'events' })

      expect(result).toEqual(mockTableMetadata)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables/events',
        headers: {},
      })
    })

    it('should load table from multipart namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.loadTable({ namespace: ['analytics', 'prod'], name: 'events' })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics%1Fprod/tables/events',
        headers: {},
      })
    })

    it('should send If-None-Match when ifNoneMatch is provided', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers({ etag: '"v1"' }),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.loadTable({ namespace: ['analytics'], name: 'events' }, { ifNoneMatch: '"v0"' })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables/events',
        headers: { 'If-None-Match': '"v0"' },
      })
    })

    it('should return null when server responds 304', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 304,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.loadTable(
        { namespace: ['analytics'], name: 'events' },
        { ifNoneMatch: '"v0"' }
      )

      expect(result).toBeNull()
    })
  })

  describe('loadTableResult', () => {
    it('should expose etag from the response headers', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers({ etag: '"abc"' }),
        data: { metadata: mockTableMetadata, 'metadata-location': 's3://bucket/m.json' },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.loadTableResult({ namespace: ['analytics'], name: 'events' })

      expect(result?.etag).toBe('"abc"')
      expect(result?.metadata).toEqual(mockTableMetadata)
      expect(result?.['metadata-location']).toBe('s3://bucket/m.json')
    })

    it('should return null on 304', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 304,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.loadTableResult(
        { namespace: ['analytics'], name: 'events' },
        { ifNoneMatch: '"v0"' }
      )

      expect(result).toBeNull()
    })
  })

  describe('updateTable', () => {
    it('should send a spec-aligned commit body and emit Idempotency-Key', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          'metadata-location': 's3://bucket/warehouse/analytics/events/metadata/v1.json',
          metadata: mockTableMetadata,
        },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.updateTable(
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

      expect(result).toEqual({
        'metadata-location': 's3://bucket/warehouse/analytics/events/metadata/v1.json',
        metadata: mockTableMetadata,
      })
      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.method).toBe('POST')
      expect(callArg.path).toBe('/v1/namespaces/analytics/tables/events')
      expect(callArg.body).toEqual({
        requirements: [],
        updates: [{ action: 'set-properties', updates: { 'read.split.target-size': '134217728' } }],
      })
      expect(callArg.headers?.['Idempotency-Key']).toMatch(UUID_RE)
    })

    it('should accept an add-schema + set-current-schema sequence', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          'metadata-location': 's3://bucket/m.json',
          metadata: mockTableMetadata,
        },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.updateTable(
        { namespace: ['analytics'], name: 'events' },
        {
          requirements: [{ type: 'assert-current-schema-id', 'current-schema-id': 0 }],
          updates: [
            {
              action: 'add-schema',
              schema: {
                type: 'struct',
                fields: [{ id: 1, name: 'id', type: 'long', required: true }],
                'schema-id': 1,
              },
            },
            { action: 'set-current-schema', 'schema-id': 1 },
          ],
        }
      )

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.body).toMatchObject({
        requirements: [{ type: 'assert-current-schema-id', 'current-schema-id': 0 }],
        updates: [{ action: 'add-schema' }, { action: 'set-current-schema', 'schema-id': 1 }],
      })
    })
  })

  describe('dropTable', () => {
    it('should drop a table with default purge=false and emit Idempotency-Key', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.dropTable({ namespace: ['analytics'], name: 'events' })

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.method).toBe('DELETE')
      expect(callArg.path).toBe('/v1/namespaces/analytics/tables/events')
      expect(callArg.query).toEqual({ purgeRequested: 'false' })
      expect(callArg.headers?.['Idempotency-Key']).toMatch(UUID_RE)
    })

    it('should drop a table with purge=true', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.dropTable({ namespace: ['analytics'], name: 'events' }, { purge: true })

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.query).toEqual({ purgeRequested: 'true' })
    })
  })

  describe('tableExists', () => {
    it('should return true when table exists', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.tableExists({ namespace: ['analytics'], name: 'events' })

      expect(result).toBe(true)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'HEAD',
        path: '/v1/namespaces/analytics/tables/events',
        headers: {},
      })
    })

    it('should return false when table does not exist', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockRejectedValue(
        new IcebergError('Not Found', { status: 404 })
      )

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.tableExists({ namespace: ['analytics'], name: 'events' })

      expect(result).toBe(false)
    })

    it('should re-throw non-404 errors', async () => {
      const mockClient = createMockClient()
      const error = new IcebergError('Server Error', { status: 500 })
      vi.mocked(mockClient.request).mockRejectedValue(error)

      const ops = new TableOperations(mockClient, '/v1')

      await expect(ops.tableExists({ namespace: ['analytics'], name: 'events' })).rejects.toThrow(
        error
      )
    })
  })

  describe('createTableIfNotExists', () => {
    it('should create table if it does not exist', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.createTableIfNotExists(
        { namespace: ['analytics'] },
        {
          name: 'events',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', type: 'long', required: true }],
            'schema-id': 0,
          },
        }
      )

      expect(result).toEqual(mockTableMetadata)
    })

    it('should return existing table metadata if already exists', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request)
        .mockRejectedValueOnce(new IcebergError('Table already exists', { status: 409 }))
        .mockResolvedValueOnce({
          status: 200,
          headers: new Headers(),
          data: { metadata: mockTableMetadata },
        })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.createTableIfNotExists(
        { namespace: ['analytics'] },
        {
          name: 'events',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', type: 'long', required: true }],
            'schema-id': 0,
          },
        }
      )

      expect(result).toEqual(mockTableMetadata)
    })

    it('should re-throw non-409 errors', async () => {
      const mockClient = createMockClient()
      const error = new IcebergError('Server Error', { status: 500 })
      vi.mocked(mockClient.request).mockRejectedValue(error)

      const ops = new TableOperations(mockClient, '/v1')

      await expect(
        ops.createTableIfNotExists(
          { namespace: ['analytics'] },
          {
            name: 'events',
            schema: {
              type: 'struct',
              fields: [{ id: 1, name: 'id', type: 'long', required: true }],
              'schema-id': 0,
            },
          }
        )
      ).rejects.toThrow(error)
    })
  })

  describe('accessDelegation', () => {
    it('should include X-Iceberg-Access-Delegation header on createTable', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1', 'vended-credentials')
      await ops.createTable(
        { namespace: ['analytics'] },
        {
          name: 'events',
          schema: {
            type: 'struct',
            fields: [{ id: 1, name: 'id', type: 'long', required: true }],
            'schema-id': 0,
          },
        }
      )

      const callArg = vi.mocked(mockClient.request).mock.calls[0][0]
      expect(callArg.headers?.['X-Iceberg-Access-Delegation']).toBe('vended-credentials')
    })

    it('should include X-Iceberg-Access-Delegation header on loadTable', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1', 'vended-credentials,remote-signing')
      await ops.loadTable({ namespace: ['analytics'], name: 'events' })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables/events',
        headers: { 'X-Iceberg-Access-Delegation': 'vended-credentials,remote-signing' },
      })
    })
  })
})
