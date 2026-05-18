import type { HttpClient } from '../http/types'
import { IcebergError } from '../errors/IcebergError'
import { generateIdempotencyKey } from '../utils/idempotency-key'
import type {
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  GetNamespaceResponse,
  ListNamespacesOptions,
  ListNamespacesResponse,
  ListNamespacesResult,
  NamespaceIdentifier,
  NamespaceMetadata,
  UpdateNamespacePropertiesRequest,
  UpdateNamespacePropertiesResponse,
} from './types'

/**
 * Encode a multipart namespace for use as a single URL path segment.
 *
 * Each part is encoded with `encodeURIComponent` (so `/`, `?`, `#`, `%` etc.
 * inside a part survive intact), and parts are joined with the literal `%1F`
 * separator the spec calls for. Note: relying on the URL constructor to
 * encode a raw `\x1F` byte does the right thing for the separator but does
 * NOT encode `/`, `?`, `#`, `%` inside a part — hence the per-part
 * `encodeURIComponent` here.
 *
 * For the `parent` *query* parameter (not path), the raw `\x1F` join is the
 * correct input — `URLSearchParams.set` percent-encodes it to `%1F` and
 * pre-encoding would double-encode the `%` to `%25`.
 */
function namespaceToPath(namespace: string[]): string {
  return namespace.map(encodeURIComponent).join('%1F')
}

function namespaceToParentQuery(namespace: string[]): string {
  // Raw 0x1F — URLSearchParams encodes to %1F. Do NOT pre-encode parts here.
  return namespace.join('\x1F')
}

/**
 * A prefix resolver. Operations classes accept this so the catalog can defer
 * the choice of path prefix until /v1/config has been fetched (the spec
 * recommends warehouse → server-returned `prefix` over hardcoded names).
 *
 * For simple usage and tests, pass a string and it'll be wrapped automatically.
 */
export type PrefixResolver = string | (() => string | Promise<string>)

async function resolvePrefix(p: PrefixResolver): Promise<string> {
  return typeof p === 'string' ? p : p()
}

export class NamespaceOperations {
  constructor(
    private readonly client: HttpClient,
    private readonly prefix: PrefixResolver = ''
  ) {}

  async listNamespaces(options: ListNamespacesOptions = {}): Promise<ListNamespacesResult> {
    const prefix = await resolvePrefix(this.prefix)

    const query: Record<string, string | undefined> = {}
    if (options.parent) query.parent = namespaceToParentQuery(options.parent.namespace)
    if (options.pageToken !== undefined) query.pageToken = options.pageToken
    if (options.pageSize !== undefined) query.pageSize = String(options.pageSize)
    const hasQuery = Object.keys(query).some((k) => query[k] !== undefined)

    const response = await this.client.request<ListNamespacesResponse>({
      method: 'GET',
      path: `${prefix}/namespaces`,
      query: hasQuery ? query : undefined,
    })

    return {
      namespaces: (response.data.namespaces ?? []).map((ns) => ({ namespace: ns })),
      nextPageToken: response.data['next-page-token'] ?? undefined,
    }
  }

  async createNamespace(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse> {
    const prefix = await resolvePrefix(this.prefix)
    const request: CreateNamespaceRequest = {
      namespace: id.namespace,
      properties: metadata?.properties,
    }

    const response = await this.client.request<CreateNamespaceResponse>({
      method: 'POST',
      path: `${prefix}/namespaces`,
      body: request,
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })

    return response.data
  }

  async dropNamespace(id: NamespaceIdentifier): Promise<void> {
    const prefix = await resolvePrefix(this.prefix)
    await this.client.request<void>({
      method: 'DELETE',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}`,
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })
  }

  async loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata> {
    const prefix = await resolvePrefix(this.prefix)
    const response = await this.client.request<GetNamespaceResponse>({
      method: 'GET',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}`,
    })

    return {
      properties: response.data.properties ?? {},
    }
  }

  async namespaceExists(id: NamespaceIdentifier): Promise<boolean> {
    const prefix = await resolvePrefix(this.prefix)
    try {
      await this.client.request<void>({
        method: 'HEAD',
        path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}`,
      })
      return true
    } catch (error) {
      if (error instanceof IcebergError && error.status === 404) {
        return false
      }
      throw error
    }
  }

  async updateNamespaceProperties(
    id: NamespaceIdentifier,
    request: UpdateNamespacePropertiesRequest
  ): Promise<UpdateNamespacePropertiesResponse> {
    const prefix = await resolvePrefix(this.prefix)
    const response = await this.client.request<UpdateNamespacePropertiesResponse>({
      method: 'POST',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}/properties`,
      body: request,
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })
    return response.data
  }

  async createNamespaceIfNotExists(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse | void> {
    try {
      return await this.createNamespace(id, metadata)
    } catch (error) {
      if (error instanceof IcebergError && error.status === 409) {
        return
      }
      throw error
    }
  }
}
