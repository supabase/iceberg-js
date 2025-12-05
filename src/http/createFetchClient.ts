import { IcebergError, type IcebergErrorResponse } from '../errors/IcebergError'
import { buildUrl } from '../utils/url'
import type { AuthConfig, HttpClient, HttpRequest, HttpResponse } from './types'

async function buildAuthHeaders(auth?: AuthConfig): Promise<Record<string, string>> {
  if (!auth || auth.type === 'none') {
    return {}
  }

  if (auth.type === 'bearer') {
    return { Authorization: `Bearer ${auth.token}` }
  }

  if (auth.type === 'header') {
    return { [auth.name]: auth.value }
  }

  if (auth.type === 'custom') {
    return await auth.getHeaders()
  }

  return {}
}

export function createFetchClient(options: {
  baseUrl: string
  auth?: AuthConfig
  fetchImpl?: typeof fetch
}): HttpClient {
  const fetchFn = options.fetchImpl ?? globalThis.fetch

  return {
    async request<T>({
      method,
      path,
      query,
      body,
      headers,
    }: HttpRequest): Promise<HttpResponse<T>> {
      const url = buildUrl(options.baseUrl, path, query)
      const authHeaders = await buildAuthHeaders(options.auth)

      const res = await fetchFn(url, {
        method,
        headers: {
          ...(body ? { 'Content-Type': 'application/json' } : {}),
          ...authHeaders,
          ...headers,
        },
        body: body ? JSON.stringify(body) : undefined,
      })

      const text = await res.text()
      const isJson = (res.headers.get('content-type') || '').includes('application/json')
      const data = isJson && text ? (JSON.parse(text) as T) : (text as T)

      if (!res.ok) {
        const errBody = isJson ? (data as IcebergErrorResponse) : undefined
        const errorDetail = errBody?.error
        throw new IcebergError(errorDetail?.message ?? `Request failed with status ${res.status}`, {
          status: res.status,
          icebergType: errorDetail?.type,
          icebergCode: errorDetail?.code,
          details: errBody,
        })
      }

      return { status: res.status, headers: res.headers, data: data as T }
    },
  }
}
