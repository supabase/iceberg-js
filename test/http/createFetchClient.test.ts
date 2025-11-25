import { describe, it, expect, vi } from 'vitest'
import { createFetchClient } from '../../src/http/createFetchClient'
import { IcebergError } from '../../src/errors/IcebergError'

describe('createFetchClient', () => {
  it('should make a successful GET request', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({ data: 'test' }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    const response = await client.request({
      method: 'GET',
      path: '/test',
    })

    expect(response.status).toBe(200)
    expect(response.data).toEqual({ data: 'test' })
    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/test',
      expect.objectContaining({
        method: 'GET',
      })
    )
  })

  it('should make a POST request with body', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({ id: 123 }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    const response = await client.request({
      method: 'POST',
      path: '/items',
      body: { name: 'test' },
    })

    expect(response.status).toBe(201)
    expect(response.data).toEqual({ id: 123 })
    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/items',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ name: 'test' }),
        headers: expect.objectContaining({
          'Content-Type': 'application/json',
        }),
      })
    )
  })

  it('should add bearer token authentication', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      auth: { type: 'bearer', token: 'secret-token' },
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/protected',
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/protected',
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: 'Bearer secret-token',
        }),
      })
    )
  })

  it('should add custom header authentication', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      auth: { type: 'header', name: 'X-API-Key', value: 'my-key' },
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/api',
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({
        headers: expect.objectContaining({
          'X-API-Key': 'my-key',
        }),
      })
    )
  })

  it('should handle custom auth function', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      auth: {
        type: 'custom',
        getHeaders: async () => ({ 'X-Custom': 'value' }),
      },
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/custom',
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/custom',
      expect.objectContaining({
        headers: expect.objectContaining({
          'X-Custom': 'value',
        }),
      })
    )
  })

  it('should include query parameters', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/items',
      query: { page: '1', size: '10' },
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/items?page=1&size=10',
      expect.anything()
    )
  })

  it('should skip undefined query parameters', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/items',
      query: { page: '1', filter: undefined },
    })

    expect(mockFetch).toHaveBeenCalledWith('https://example.com/items?page=1', expect.anything())
  })

  it('should throw IcebergError on 404', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Table not found',
            type: 'NoSuchTableException',
            code: 404,
          },
        }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await expect(
      client.request({
        method: 'GET',
        path: '/table',
      })
    ).rejects.toThrow(IcebergError)

    try {
      await client.request({
        method: 'GET',
        path: '/table',
      })
    } catch (error) {
      expect(error).toBeInstanceOf(IcebergError)
      if (error instanceof IcebergError) {
        expect(error.status).toBe(404)
        expect(error.message).toBe('Table not found')
        expect(error.icebergType).toBe('NoSuchTableException')
        expect(error.icebergCode).toBe(404)
      }
    }
  })

  it('should throw IcebergError with generic message on non-JSON error', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      headers: new Headers({ 'content-type': 'text/plain' }),
      text: async () => 'Internal Server Error',
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await expect(
      client.request({
        method: 'GET',
        path: '/error',
      })
    ).rejects.toThrow(IcebergError)

    try {
      await client.request({
        method: 'GET',
        path: '/error',
      })
    } catch (error) {
      if (error instanceof IcebergError) {
        expect(error.status).toBe(500)
        expect(error.message).toBe('Request failed with status 500')
      }
    }
  })

  it('should handle non-JSON response body', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'text/plain' }),
      text: async () => 'plain text response',
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    const response = await client.request({
      method: 'GET',
      path: '/text',
    })

    expect(response.status).toBe(200)
    expect(response.data).toBe('plain text response')
  })

  it('should detect CommitStateUnknownException for 500 errors', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Internal Server Error',
            type: 'CommitStateUnknownException',
            code: 500,
          },
        }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    try {
      await client.request({
        method: 'POST',
        path: '/tables/test',
      })
    } catch (error) {
      expect(error).toBeInstanceOf(IcebergError)
      if (error instanceof IcebergError) {
        expect(error.status).toBe(500)
        expect(error.icebergType).toBe('CommitStateUnknownException')
        expect(error.isCommitStateUnknown).toBe(true)
      }
    }
  })

  it('should detect CommitStateUnknownException for 502 errors', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 502,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Invalid response from the upstream server',
            type: 'CommitStateUnknownException',
            code: 502,
          },
        }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    try {
      await client.request({
        method: 'POST',
        path: '/tables/test',
      })
    } catch (error) {
      expect(error).toBeInstanceOf(IcebergError)
      if (error instanceof IcebergError) {
        expect(error.status).toBe(502)
        expect(error.isCommitStateUnknown).toBe(true)
      }
    }
  })

  it('should detect CommitStateUnknownException for 504 errors', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 504,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Gateway timed out during commit',
            type: 'CommitStateUnknownException',
            code: 504,
          },
        }),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    try {
      await client.request({
        method: 'POST',
        path: '/tables/test',
      })
    } catch (error) {
      expect(error).toBeInstanceOf(IcebergError)
      if (error instanceof IcebergError) {
        expect(error.status).toBe(504)
        expect(error.isCommitStateUnknown).toBe(true)
      }
    }
  })

  it('should test helper methods isNotFound, isConflict, isAuthenticationTimeout', async () => {
    const mockFetch404 = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Not found',
            type: 'NoSuchTableException',
            code: 404,
          },
        }),
    })

    const client404 = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch404 as unknown as typeof fetch,
    })

    try {
      await client404.request({ method: 'GET', path: '/test' })
    } catch (error) {
      if (error instanceof IcebergError) {
        expect(error.isNotFound()).toBe(true)
        expect(error.isConflict()).toBe(false)
        expect(error.isAuthenticationTimeout()).toBe(false)
      }
    }

    const mockFetch409 = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Already exists',
            type: 'AlreadyExistsException',
            code: 409,
          },
        }),
    })

    const client409 = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch409 as unknown as typeof fetch,
    })

    try {
      await client409.request({ method: 'POST', path: '/test' })
    } catch (error) {
      if (error instanceof IcebergError) {
        expect(error.isNotFound()).toBe(false)
        expect(error.isConflict()).toBe(true)
        expect(error.isAuthenticationTimeout()).toBe(false)
      }
    }

    const mockFetch419 = vi.fn().mockResolvedValue({
      ok: false,
      status: 419,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () =>
        JSON.stringify({
          error: {
            message: 'Authentication timeout',
            type: 'AuthenticationTimeoutException',
            code: 419,
          },
        }),
    })

    const client419 = createFetchClient({
      baseUrl: 'https://example.com',
      fetchImpl: mockFetch419 as unknown as typeof fetch,
    })

    try {
      await client419.request({ method: 'GET', path: '/test' })
    } catch (error) {
      if (error instanceof IcebergError) {
        expect(error.isNotFound()).toBe(false)
        expect(error.isConflict()).toBe(false)
        expect(error.isAuthenticationTimeout()).toBe(true)
      }
    }
  })

  it('should merge custom headers with auth headers', async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers({ 'content-type': 'application/json' }),
      text: async () => JSON.stringify({}),
    })

    const client = createFetchClient({
      baseUrl: 'https://example.com',
      auth: { type: 'bearer', token: 'token' },
      fetchImpl: mockFetch as unknown as typeof fetch,
    })

    await client.request({
      method: 'GET',
      path: '/api',
      headers: { 'X-Custom': 'header' },
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.com/api',
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: 'Bearer token',
          'X-Custom': 'header',
        }),
      })
    )
  })
})
