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
    describe('without warehouse', () => {
      it('should not call /v1/config when warehouse is not provided', async () => {
        const catalog = createCatalog({})

        // Mock namespace list response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['default']] }))

        await catalog.listNamespaces()

        // Should only have one call (listNamespaces), no config call
        expect(mockFetch).toHaveBeenCalledTimes(1)
        expect(mockFetch).toHaveBeenCalledWith(
          'https://catalog.example.com/v1/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })
    })

    describe('with warehouse', () => {
      it('should call /v1/config with warehouse query param on first operation', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        // Mock config response (no prefix override)
        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {},
          })
        )
        // Mock namespace list response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['default']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)

        // First call should be config with warehouse query param
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )

        // Second call should be namespace list
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should apply prefix override from config response', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        // Mock config response WITH prefix override
        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'account123/bucket456',
            },
          })
        )
        // Mock namespace list response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

        await catalog.listNamespaces()

        expect(mockFetch).toHaveBeenCalledTimes(2)

        // First call should be config with warehouse query param
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )

        // Second call should use the prefix from config
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/account123/bucket456/namespaces',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should only call config once (lazy initialization)', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        // Mock config response WITH prefix override
        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: {},
            overrides: {
              prefix: 'account123/bucket456',
            },
          })
        )
        // Mock namespace list response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))
        // Mock listTables response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ identifiers: ['my_table'] }))

        await catalog.listNamespaces()
        await catalog.listTables({ namespace: ['analytics'] })

        expect(mockFetch).toHaveBeenCalledTimes(3)

        // First call should be config with warehouse query param
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )

        // Second call should use the prefix from config
        expect(mockFetch).toHaveBeenNthCalledWith(
          2,
          'https://catalog.example.com/v1/account123/bucket456/namespaces',
          expect.objectContaining({ method: 'GET' })
        )

        // Third call should be listTables with prefix (no additional config call)
        expect(mockFetch).toHaveBeenNthCalledWith(
          3,
          'https://catalog.example.com/v1/account123/bucket456/namespaces/analytics/tables',
          expect.objectContaining({ method: 'GET' })
        )
      })

      it('should work without prefix in config response', async () => {
        const catalog = createCatalog({ warehouse: 'my-warehouse' })

        // Mock config response without prefix
        mockFetch.mockReturnValueOnce(
          mockFetchResponse({
            defaults: { 'some-default': 'value' },
            overrides: { 'some-override': 'value' },
            // No prefix key
          })
        )
        // Mock namespace list response
        mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [] }))

        await catalog.listNamespaces()

        // First call should be config with warehouse query param
        expect(mockFetch).toHaveBeenNthCalledWith(
          1,
          'https://catalog.example.com/v1/config?warehouse=my-warehouse',
          expect.objectContaining({ method: 'GET' })
        )

        // Should use default v1 prefix (no extra path segment)
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

      // Mock namespace list response
      mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['default']] }))

      await catalog.listNamespaces()

      // Should only have one call (listNamespaces), no config call
      expect(mockFetch).toHaveBeenCalledTimes(1)
      expect(mockFetch).toHaveBeenCalledWith(
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

      // Mock config response WITH prefix override
      mockFetch.mockReturnValueOnce(
        mockFetchResponse({
          defaults: {},
          overrides: {
            prefix: 'server-provided-prefix',
          },
        })
      )
      // Mock namespace list response
      mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

      await catalog.listNamespaces()

      expect(mockFetch).toHaveBeenCalledTimes(2)

      // First call should be config with warehouse query param
      expect(mockFetch).toHaveBeenNthCalledWith(
        1,
        'https://catalog.example.com/v1/config?warehouse=my-warehouse',
        expect.objectContaining({ method: 'GET' })
      )

      // Second call should use server-provided prefix (NOT catalogName)
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

      // Mock config response WITHOUT prefix override
      mockFetch.mockReturnValueOnce(
        mockFetchResponse({
          defaults: {},
          overrides: {},
        })
      )
      // Mock namespace list response
      mockFetch.mockReturnValueOnce(mockFetchResponse({ namespaces: [['analytics']] }))

      await catalog.listNamespaces()

      expect(mockFetch).toHaveBeenCalledTimes(2)

      // First call should be config with warehouse query param
      expect(mockFetch).toHaveBeenNthCalledWith(
        1,
        'https://catalog.example.com/v1/config?warehouse=my-warehouse',
        expect.objectContaining({ method: 'GET' })
      )

      // Second call should still use catalogName since no prefix override
      expect(mockFetch).toHaveBeenNthCalledWith(
        2,
        'https://catalog.example.com/v1/initial-catalog/namespaces',
        expect.objectContaining({ method: 'GET' })
      )
    })
  })
})
