import { describe, it, expect, vi } from 'vitest'
import { IcebergRestCatalog } from '../../src/catalog/IcebergRestCatalog'

describe('IcebergRestCatalog', () => {
  function makeFetch(impl: (url: string, init: RequestInit) => Response | Promise<Response>) {
    return vi.fn(impl) as unknown as typeof fetch
  }

  function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
    return new Response(JSON.stringify(body), {
      status,
      headers: { 'content-type': 'application/json', ...headers },
    })
  }

  describe('warehouse → /v1/config flow', () => {
    it('fetches /v1/config?warehouse=… on first use and uses the server-returned prefix', async () => {
      const seen: string[] = []
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        seen.push(u)
        if (u.includes('/v1/config')) {
          return jsonResponse(200, {
            defaults: {},
            overrides: { prefix: 'tenant-42' },
          })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'my-warehouse',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()

      expect(seen[0]).toBe('https://catalog.example.com/v1/config?warehouse=my-warehouse')
      expect(seen[1]).toBe('https://catalog.example.com/v1/tenant-42/namespaces')
    })

    it('memoizes the /v1/config call', async () => {
      let configCalls = 0
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        if (u.includes('/v1/config')) {
          configCalls++
          return jsonResponse(200, { defaults: {}, overrides: { prefix: 'tenant' } })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'wh',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()
      await catalog.listNamespaces()
      await catalog.listNamespaces()

      expect(configCalls).toBe(1)
    })

    it('falls back to using the warehouse as the prefix when /v1/config is unavailable', async () => {
      const seen: string[] = []
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        seen.push(u)
        if (u.includes('/v1/config')) {
          return jsonResponse(404, {
            error: { message: 'no config', type: 'NoSuchWarehouseException', code: 404 },
          })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'wh',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()

      expect(seen[1]).toBe('https://catalog.example.com/v1/wh/namespaces')
    })

    it('uses the warehouse literally when /v1/config returns no prefix override', async () => {
      const seen: string[] = []
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        seen.push(u)
        if (u.includes('/v1/config')) {
          return jsonResponse(200, { defaults: {}, overrides: {} })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'wh',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()

      expect(seen[1]).toBe('https://catalog.example.com/v1/wh/namespaces')
    })
  })

  describe('catalogName backward-compat', () => {
    it('treats catalogName as an alias for warehouse', async () => {
      const seen: string[] = []
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        seen.push(u)
        if (u.includes('/v1/config')) {
          return jsonResponse(200, { defaults: {}, overrides: { prefix: 'srv' } })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        catalogName: 'legacy-name',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()

      expect(seen[0]).toBe('https://catalog.example.com/v1/config?warehouse=legacy-name')
      expect(seen[1]).toBe('https://catalog.example.com/v1/srv/namespaces')
    })

    it('prefers warehouse when both are set', async () => {
      const fetchImpl = makeFetch(async (url) => {
        const u = String(url)
        if (u.includes('/v1/config')) {
          expect(u).toContain('warehouse=primary')
          return jsonResponse(200, { defaults: {}, overrides: {} })
        }
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'primary',
        catalogName: 'legacy',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()
    })
  })

  describe('no warehouse', () => {
    it('does not call /v1/config and uses bare v1 prefix', async () => {
      const seen: string[] = []
      const fetchImpl = makeFetch(async (url) => {
        seen.push(String(url))
        return jsonResponse(200, { namespaces: [] })
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        fetch: fetchImpl,
      })

      await catalog.listNamespaces()

      expect(seen).toEqual(['https://catalog.example.com/v1/namespaces'])
    })
  })

  describe('loadConfig()', () => {
    it('returns the cached config object', async () => {
      const fetchImpl = makeFetch(async (url) => {
        if (String(url).includes('/v1/config')) {
          return jsonResponse(200, {
            defaults: { clients: '4' },
            overrides: { prefix: 'p' },
            endpoints: ['GET /v1/{prefix}/namespaces'],
          })
        }
        return jsonResponse(200, {})
      })

      const catalog = new IcebergRestCatalog({
        baseUrl: 'https://catalog.example.com',
        warehouse: 'wh',
        fetch: fetchImpl,
      })

      const a = await catalog.loadConfig()
      const b = await catalog.loadConfig()

      expect(a).toBe(b)
      expect(a.overrides.prefix).toBe('p')
      expect(a.endpoints).toEqual(['GET /v1/{prefix}/namespaces'])
    })
  })
})
