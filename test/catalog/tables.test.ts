import { describe, it, expect, vi } from 'vitest'
import { TableOperations } from '../../src/catalog/tables'
import type { HttpClient } from '../../src/http/types'

describe('TableOperations', () => {
  const createMockClient = (): HttpClient => ({
    request: vi.fn(),
  })

  const mockTableMetadata = {
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
    'partition-specs': [
      {
        'spec-id': 0,
        fields: [],
      },
    ],
    'sort-orders': [
      {
        'order-id': 0,
        fields: [],
      },
    ],
    properties: {},
    'metadata-location': 's3://bucket/warehouse/analytics/events/metadata/v1.json',
  }

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

      expect(result).toEqual([
        { namespace: ['analytics'], name: 'events' },
        { namespace: ['analytics'], name: 'users' },
      ])
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics/tables',
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
        path: '/v1/namespaces/analytics\x1Fprod/tables',
      })
    })

    it('should use prefix when provided', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { identifiers: [] },
      })

      const ops = new TableOperations(mockClient, '/v1/catalog1')
      await ops.listTables({ namespace: ['analytics'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/catalog1/namespaces/analytics/tables',
      })
    })
  })

  describe('createTable', () => {
    it('should create a table', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          metadata: mockTableMetadata,
        },
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
          'partition-spec': {
            'spec-id': 0,
            fields: [],
          },
        }
      )

      expect(result).toEqual(mockTableMetadata)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables',
        body: expect.objectContaining({
          name: 'events',
          schema: expect.any(Object),
        }),
        headers: {},
      })
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
            fields: [
              {
                source_id: 2,
                field_id: 1000,
                name: 'ts_day',
                transform: 'day',
              },
            ],
          },
        }
      )

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables',
        body: expect.objectContaining({
          'partition-spec': {
            'spec-id': 0,
            fields: [
              {
                source_id: 2,
                field_id: 1000,
                name: 'ts_day',
                transform: 'day',
              },
            ],
          },
        }),
        headers: {},
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
          schema: {
            type: 'struct',
            fields: [],
            'schema-id': 0,
          },
          properties: {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'snappy',
          },
        }
      )

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables',
        body: expect.objectContaining({
          properties: {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'snappy',
          },
        }),
        headers: {},
      })
    })
  })

  describe('loadTable', () => {
    it('should load a table', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          metadata: mockTableMetadata,
        },
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
        path: '/v1/namespaces/analytics\x1Fprod/tables/events',
        headers: {},
      })
    })
  })

  describe('updateTable', () => {
    it('should update table properties', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      const result = await ops.updateTable(
        { namespace: ['analytics'], name: 'events' },
        {
          properties: { 'read.split.target-size': '134217728' },
        }
      )

      expect(result).toEqual(mockTableMetadata)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables/events',
        body: {
          properties: { 'read.split.target-size': '134217728' },
        },
      })
    })

    it('should update table schema', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { metadata: mockTableMetadata },
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.updateTable(
        { namespace: ['analytics'], name: 'events' },
        {
          schema: {
            type: 'struct',
            fields: [
              { id: 1, name: 'id', type: 'long', required: true },
              { id: 2, name: 'timestamp', type: 'timestamp', required: true },
              { id: 3, name: 'user_id', type: 'string', required: false },
            ],
            'schema-id': 1,
          },
        }
      )

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables/events',
        body: expect.objectContaining({
          schema: expect.objectContaining({
            'schema-id': 1,
          }),
        }),
      })
    })
  })

  describe('dropTable', () => {
    it('should drop a table', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.dropTable({ namespace: ['analytics'], name: 'events' })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'DELETE',
        path: '/v1/namespaces/analytics/tables/events',
        query: { purgeRequested: 'false' },
      })
    })

    it('should drop a table with purge set to true', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.dropTable({ namespace: ['analytics'], name: 'events' }, { purge: true })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'DELETE',
        path: '/v1/namespaces/analytics/tables/events',
        query: { purgeRequested: 'true' },
      })
    })

    it('should drop table from multipart namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new TableOperations(mockClient, '/v1')
      await ops.dropTable({ namespace: ['analytics', 'prod'], name: 'events' })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'DELETE',
        path: '/v1/namespaces/analytics\x1Fprod/tables/events',
        query: { purgeRequested: 'false' },
      })
    })
  })

  describe('accessDelegation', () => {
    it('should include X-Iceberg-Access-Delegation header when creating table', async () => {
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

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables',
        body: expect.any(Object),
        headers: {
          'X-Iceberg-Access-Delegation': 'vended-credentials',
        },
      })
    })

    it('should include X-Iceberg-Access-Delegation header when loading table', async () => {
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
        headers: {
          'X-Iceberg-Access-Delegation': 'vended-credentials,remote-signing',
        },
      })
    })

    it('should not include header when accessDelegation is not set', async () => {
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
        }
      )

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces/analytics/tables',
        body: expect.any(Object),
        headers: {},
      })
    })
  })
})
