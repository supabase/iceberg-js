import { describe, it, expect, vi } from 'vitest'
import { NamespaceOperations } from '../../src/catalog/namespaces'
import { IcebergError } from '../../src/errors/IcebergError'
import type { HttpClient } from '../../src/http/types'

describe('NamespaceOperations', () => {
  const createMockClient = (): HttpClient => ({
    request: vi.fn(),
  })

  describe('listNamespaces', () => {
    it('should list all namespaces', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespaces: [['default'], ['analytics'], ['logs']],
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.listNamespaces()

      expect(result).toEqual([
        { namespace: ['default'] },
        { namespace: ['analytics'] },
        { namespace: ['logs'] },
      ])
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces',
        query: undefined,
      })
    })

    it('should list namespaces under a parent', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespaces: [
            ['analytics', 'prod'],
            ['analytics', 'dev'],
          ],
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.listNamespaces({ namespace: ['analytics'] })

      expect(result).toEqual([
        { namespace: ['analytics', 'prod'] },
        { namespace: ['analytics', 'dev'] },
      ])
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces',
        query: { parent: 'analytics' },
      })
    })

    it('should handle multipart namespace parent with separator', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespaces: [['a', 'b', 'c']],
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.listNamespaces({ namespace: ['a', 'b'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces',
        query: { parent: 'a\x1Fb' },
      })
    })

    it('should use prefix when provided', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { namespaces: [] },
      })

      const ops = new NamespaceOperations(mockClient, '/v1/catalog1')
      await ops.listNamespaces()

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/catalog1/namespaces',
        query: undefined,
      })
    })
  })

  describe('createNamespace', () => {
    it('should create a namespace without properties', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespace: ['analytics'],
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.createNamespace({ namespace: ['analytics'] })

      expect(result).toEqual({ namespace: ['analytics'] })
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces',
        body: {
          namespace: ['analytics'],
          properties: undefined,
        },
      })
    })

    it('should create a namespace with properties', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespace: ['analytics'],
          properties: { owner: 'team' },
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.createNamespace(
        { namespace: ['analytics'] },
        { properties: { owner: 'team' } }
      )

      expect(result).toEqual({
        namespace: ['analytics'],
        properties: { owner: 'team' },
      })
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces',
        body: {
          namespace: ['analytics'],
          properties: { owner: 'team' },
        },
      })
    })

    it('should create multipart namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: { namespace: ['analytics', 'prod'] },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.createNamespace({ namespace: ['analytics', 'prod'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces',
        body: {
          namespace: ['analytics', 'prod'],
          properties: undefined,
        },
      })
    })
  })

  describe('dropNamespace', () => {
    it('should drop a namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.dropNamespace({ namespace: ['analytics'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'DELETE',
        path: '/v1/namespaces/analytics',
      })
    })

    it('should drop multipart namespace with separator', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 204,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.dropNamespace({ namespace: ['analytics', 'prod'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'DELETE',
        path: '/v1/namespaces/analytics\x1Fprod',
      })
    })
  })

  describe('loadNamespaceMetadata', () => {
    it('should load namespace metadata', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespace: ['analytics'],
          properties: {
            owner: 'data-team',
            description: 'Analytics namespace',
          },
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.loadNamespaceMetadata({ namespace: ['analytics'] })

      expect(result).toEqual({
        properties: {
          owner: 'data-team',
          description: 'Analytics namespace',
        },
      })
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics',
      })
    })

    it('should load metadata for multipart namespace', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: {
          namespace: ['analytics', 'prod'],
          properties: {},
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.loadNamespaceMetadata({ namespace: ['analytics', 'prod'] })

      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'GET',
        path: '/v1/namespaces/analytics\x1Fprod',
      })
    })
  })

  describe('namespaceExists', () => {
    it('should return true when namespace exists', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValue({
        status: 200,
        headers: new Headers(),
        data: undefined,
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.namespaceExists({ namespace: ['analytics'] })

      expect(result).toBe(true)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'HEAD',
        path: '/v1/namespaces/analytics',
      })
    })

    it('should return false when namespace does not exist', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockRejectedValue(
        new IcebergError('Not Found', { status: 404 })
      )

      const ops = new NamespaceOperations(mockClient, '/v1')
      const result = await ops.namespaceExists({ namespace: ['analytics'] })

      expect(result).toBe(false)
    })

    it('should re-throw non-404 errors', async () => {
      const mockClient = createMockClient()
      const error = new IcebergError('Server Error', { status: 500 })
      vi.mocked(mockClient.request).mockRejectedValue(error)

      const ops = new NamespaceOperations(mockClient, '/v1')

      await expect(ops.namespaceExists({ namespace: ['analytics'] })).rejects.toThrow(error)
    })
  })

  describe('createNamespaceIfNotExists', () => {
    it('should create namespace if it does not exist', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockResolvedValueOnce({
        status: 200,
        headers: new Headers(),
        data: {
          namespace: ['analytics'],
          properties: { owner: 'data-team' },
        },
      })

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.createNamespaceIfNotExists(
        { namespace: ['analytics'] },
        { properties: { owner: 'data-team' } }
      )

      expect(mockClient.request).toHaveBeenCalledTimes(1)
      expect(mockClient.request).toHaveBeenCalledWith({
        method: 'POST',
        path: '/v1/namespaces',
        body: {
          namespace: ['analytics'],
          properties: { owner: 'data-team' },
        },
      })
    })

    it('should do nothing if namespace already exists', async () => {
      const mockClient = createMockClient()
      vi.mocked(mockClient.request).mockRejectedValueOnce(
        new IcebergError('Namespace already exists', { status: 409 })
      )

      const ops = new NamespaceOperations(mockClient, '/v1')
      await ops.createNamespaceIfNotExists(
        { namespace: ['analytics'] },
        { properties: { owner: 'data-team' } }
      )

      expect(mockClient.request).toHaveBeenCalledTimes(1)
    })

    it('should re-throw non-409 errors', async () => {
      const mockClient = createMockClient()
      const error = new IcebergError('Server Error', { status: 500 })
      vi.mocked(mockClient.request).mockRejectedValue(error)

      const ops = new NamespaceOperations(mockClient, '/v1')

      await expect(ops.createNamespaceIfNotExists({ namespace: ['analytics'] })).rejects.toThrow(
        error
      )
    })
  })
})
