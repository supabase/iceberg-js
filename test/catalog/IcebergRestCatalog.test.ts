import { describe, it, expect, vi, beforeEach } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'

describe('IcebergRestCatalog', () => {
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockFetch = vi.fn()
  })

  const createCatalog = (options: {
    baseUrl?: string
    catalogName?: string
    warehouse?: string
  }) => {
    return new IcebergRestCatalog({
      baseUrl: options.baseUrl ?? 'https://catalog.example.com',
      catalogName: options.catalogName,
      warehouse: options.warehouse,
      fetch: mockFetch as unknown as typeof fetch,
    })
  }

  const mockFetchResponse = (data: unknown, status = 200) => {
    return Promise.resolve({
      ok: status >= 200 && status < 300,
      status,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: () => Promise.resolve(JSON.stringify(data)),
    })
  }

  describe('fetchConfig', () => {
    describe('lazy initialization', () => {
      it('should only call once', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))
        mockFetch.mockReturnValueOnce(mockFetchResponse({ identifiers: ['my_table'] }))

        await catalog.listNamespaces()
        await catalog.listTables({ namespace: ['analytics'] })

        expect(mockFetch).toHaveBeenCalledTimes(3)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          3,
          'https://catalog.example.com/v1/namespaces/analytics/tables',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('catalogName option', () => {
      it('should use catalogName as the prefix catalog property', async () => {
        const catalog = createCatalog({ catalogName: 'prod' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['default']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/prod/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('warehouse option', () => {
      it('should call configuration api with warehouse query param', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['default']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('catalogName + warehouse combination', () => {
      it('should use the prefix in overrides response as the prefix catalog property', async () => {
        const catalog = createCatalog({
          catalogName: 'initial-catalog',
          warehouse: 'my-warehouse',
        })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'server-provided-prefix',
            },
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/server-provided-prefix/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should use catalogName as the prefix catalog property if the prefix in overrieds does not exist', async () => {
        const catalog = createCatalog({
          catalogName: 'initial-catalog',
          warehouse: 'my-warehouse',
        })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/initial-catalog/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('catalog properties merge priority', () => {
      it('use default properties if neither client nor server provided properties', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {
              prefix: 'default-prefix'
            },
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/default-prefix/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('client-provided properties should take precedence over default properties', async () => {
        const catalog = createCatalog({ catalogName: 'client-provided-prefix' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {
              prefix: 'default-prefix'
            },
            overrides: {},
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/client-provided-prefix/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('server-provided properties should take precedence over default properties', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {
              prefix: 'default-prefix'
            },
            overrides: {
              prefix: 'server-provided-prefix'
            },
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/server-provided-prefix/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('server-provided properties should take precedence over client-provided properties', async () => {
        const catalog = createCatalog({ catalogName: 'client-provided-prefix' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'server-provided-prefix'
            },
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/server-provided-prefix/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('configuration API failed', () => {
      it('should throw error when config endpoint returns 401', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse(
            {
              error: {
                message: 'Unauthorized',
                type: 'UnauthorizedException',
                code: 401,
              },
            },
            401
          )
        )

        await expect(catalog.listNamespaces()).rejects.toThrow('Unauthorized')
      })

      it('should throw error when config endpoint returns 404', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse(
            {
              error: {
                message: 'Configuration endpoint not found',
                type: 'NoSuchResourceException',
                code: 404,
              },
            },
            404
          )
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await expect(catalog.listNamespaces()).rejects.toThrow('Configuration endpoint not found')
      })

      it('should throw error when config endpoint returns 500', async () => {
        const catalog = createCatalog({})

        mockFetch.mockReturnValueOnce(
          mockFetchResponse(
            {
              error: {
                message: 'Internal Server Error',
                type: 'InternalServerException',
                code: 500,
              },
            },
            500
          )
        )

        await expect(catalog.listNamespaces()).rejects.toThrow('Internal Server Error')
      })
    })
  })
})
