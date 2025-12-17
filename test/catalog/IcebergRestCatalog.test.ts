import { describe, it, expect, vi, beforeEach } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'

describe('IcebergRestCatalog', () => {
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockFetch = vi.fn()
  })

  const createCatalog = (options: {
    baseUrl?: string
    warehouse?: string
    catalogName?: string
  }) => {
    return new IcebergRestCatalog({
      baseUrl: options.baseUrl ?? 'https://catalog.example.com',
      warehouse: options.warehouse,
      catalogName: options.catalogName,
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

  describe('warehouse option', () => {
    describe('with warehouse', () => {
      it('should call configuration api with warehouse query param on first operation', async () => {
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

      it('should apply prefix override from config response', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'account123/bucket456',
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
          'https://catalog.example.com/v1/account123/bucket456/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should only call config once (lazy initialization)', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'account123/bucket456',
            },
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))
        mockFetch.mockReturnValueOnce(mockFetchResponse({ identifiers: ['my_table'] }))

        await catalog.listNamespaces()
        await catalog.listTables({ namespace: ['analytics'] })

        expect(mockFetch).toHaveBeenCalledTimes(3)
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/account123/bucket456/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
        expect(mockFetch).toHaveBeenNthCalledWith(
          3,
          'https://catalog.example.com/v1/account123/bucket456/namespaces/analytics/tables',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should work without prefix in config response', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: { 'some-default': 'value' },
            overrides: { 'some-override': 'value' },
            // No prefix key
          })
        )
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [] }))

        await catalog.listNamespaces()

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

    describe('without warehouse', () => {
      it('should call configuration api even when warehouse is not provided', async () => {
        const catalog = createCatalog({})

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
          'https://catalog.example.com/v1/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })
  })

  describe('catalogName option', () => {
    it('should use catalogName as prefix when provided without warehouse', async () => {
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

  describe('warehouse + catalogName combination', () => {
    it('should override catalogName with config prefix when warehouse returns prefix', async () => {
      const catalog = createCatalog({
        warehouse: 'my-warehouse',
        catalogName: 'initial-catalog',
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

    it('should keep catalogName when warehouse config has no prefix override', async () => {
      const catalog = createCatalog({
        warehouse: 'my-warehouse',
        catalogName: 'initial-catalog',
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
})
