import type { HttpClient } from '../http/types'
import { IcebergError } from '../errors/IcebergError'
import { generateIdempotencyKey } from '../utils/idempotency-key'
import type { PrefixResolver } from './namespaces'
import type {
  CommitTableRequest,
  CommitTableResponse,
  CreateTableRequest,
  DropTableRequest,
  ListTablesOptions,
  ListTablesResponse,
  ListTablesResult,
  LoadTableOptions,
  LoadTableResponse,
  LoadTableResultWithEtag,
  NamespaceIdentifier,
  RegisterTableRequest,
  RenameTableRequest,
  TableIdentifier,
  TableMetadata,
  UpdateTableRequest,
} from './types'

function namespaceToPath(namespace: string[]): string {
  return namespace.map(encodeURIComponent).join('%1F')
}

function tableSegment(name: string): string {
  return encodeURIComponent(name)
}

async function resolvePrefix(p: PrefixResolver): Promise<string> {
  return typeof p === 'string' ? p : p()
}

export class TableOperations {
  constructor(
    private readonly client: HttpClient,
    private readonly prefix: PrefixResolver = '',
    private readonly accessDelegation?: string
  ) {}

  async listTables(
    namespace: NamespaceIdentifier,
    options: ListTablesOptions = {}
  ): Promise<ListTablesResult> {
    const prefix = await resolvePrefix(this.prefix)
    const query: Record<string, string | undefined> = {}
    if (options.pageToken !== undefined) query.pageToken = options.pageToken
    if (options.pageSize !== undefined) query.pageSize = String(options.pageSize)
    const hasQuery = Object.keys(query).some((k) => query[k] !== undefined)

    const response = await this.client.request<ListTablesResponse>({
      method: 'GET',
      path: `${prefix}/namespaces/${namespaceToPath(namespace.namespace)}/tables`,
      query: hasQuery ? query : undefined,
    })

    return {
      identifiers: response.data.identifiers ?? [],
      nextPageToken: response.data['next-page-token'] ?? undefined,
    }
  }

  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    const result = await this.createTableResult(namespace, request)
    return result.metadata
  }

  /**
   * Spec-aligned `LoadTableResult` wrapper for create. Returns the full server
   * response (metadata, metadata-location, config, storage-credentials) plus
   * the captured ETag header. Use this when `accessDelegation` is set so the
   * server-vended credentials are reachable.
   */
  async createTableResult(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<LoadTableResultWithEtag> {
    const prefix = await resolvePrefix(this.prefix)
    const headers: Record<string, string> = { 'Idempotency-Key': generateIdempotencyKey() }
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }

    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${prefix}/namespaces/${namespaceToPath(namespace.namespace)}/tables`,
      body: request,
      headers,
    })

    return {
      ...response.data,
      etag: response.headers.get('etag'),
    }
  }

  async updateTable(
    id: TableIdentifier,
    request: UpdateTableRequest
  ): Promise<CommitTableResponse> {
    const prefix = await resolvePrefix(this.prefix)
    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${tableSegment(id.name)}`,
      body: request,
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })

    const metadataLocation = response.data['metadata-location']
    if (!metadataLocation) {
      // Spec requires both `metadata` and `metadata-location` on
      // CommitTableResponse. A 200 without `metadata-location` means the
      // server is misbehaving; surface it instead of silently returning ''.
      throw new IcebergError(
        'Server returned 200 without required `metadata-location` field on CommitTableResponse',
        { status: response.status, details: response.data }
      )
    }

    return {
      'metadata-location': metadataLocation,
      metadata: response.data.metadata,
    }
  }

  /** Spec-aligned alias for {@link updateTable}. */
  async commitTable(
    id: TableIdentifier,
    request: CommitTableRequest
  ): Promise<CommitTableResponse> {
    return this.updateTable(id, request)
  }

  async dropTable(id: TableIdentifier, options?: DropTableRequest): Promise<void> {
    const prefix = await resolvePrefix(this.prefix)
    await this.client.request<void>({
      method: 'DELETE',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${tableSegment(id.name)}`,
      query: { purgeRequested: String(options?.purge ?? false) },
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })
  }

  async loadTable(id: TableIdentifier): Promise<TableMetadata>
  async loadTable(id: TableIdentifier, options: LoadTableOptions): Promise<TableMetadata | null>
  async loadTable(id: TableIdentifier, options?: LoadTableOptions): Promise<TableMetadata | null> {
    const result = await this.loadTableResult(id, options)
    return result ? result.metadata : null
  }

  /**
   * Spec-aligned `LoadTableResult` wrapper. Returns the full server response
   * (metadata, metadata-location, config, storage-credentials) plus the captured
   * ETag header. Returns `null` when an `ifNoneMatch` was supplied and the server
   * answered 304.
   */
  async loadTableResult(
    id: TableIdentifier,
    options?: LoadTableOptions
  ): Promise<LoadTableResultWithEtag | null> {
    const prefix = await resolvePrefix(this.prefix)
    const headers: Record<string, string> = {}
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }
    if (options?.ifNoneMatch) {
      headers['If-None-Match'] = options.ifNoneMatch
    }
    const query: Record<string, string | undefined> = {}
    if (options?.snapshots) query.snapshots = options.snapshots

    const response = await this.client.request<LoadTableResponse>({
      method: 'GET',
      path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${tableSegment(id.name)}`,
      headers,
      query: Object.keys(query).length ? query : undefined,
    })

    if (response.status === 304) {
      return null
    }

    return {
      ...response.data,
      etag: response.headers.get('etag'),
    }
  }

  async tableExists(id: TableIdentifier): Promise<boolean> {
    const prefix = await resolvePrefix(this.prefix)
    const headers: Record<string, string> = {}
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }

    try {
      await this.client.request<void>({
        method: 'HEAD',
        path: `${prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${tableSegment(id.name)}`,
        headers,
      })
      return true
    } catch (error) {
      if (error instanceof IcebergError && error.status === 404) {
        return false
      }
      throw error
    }
  }

  async registerTable(
    namespace: NamespaceIdentifier,
    request: RegisterTableRequest
  ): Promise<TableMetadata> {
    const result = await this.registerTableResult(namespace, request)
    return result.metadata
  }

  async registerTableResult(
    namespace: NamespaceIdentifier,
    request: RegisterTableRequest
  ): Promise<LoadTableResultWithEtag> {
    const prefix = await resolvePrefix(this.prefix)
    const headers: Record<string, string> = { 'Idempotency-Key': generateIdempotencyKey() }
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }
    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${prefix}/namespaces/${namespaceToPath(namespace.namespace)}/register`,
      body: request,
      headers,
    })
    return {
      ...response.data,
      etag: response.headers.get('etag'),
    }
  }

  async renameTable(request: RenameTableRequest): Promise<void> {
    const prefix = await resolvePrefix(this.prefix)
    await this.client.request<void>({
      method: 'POST',
      path: `${prefix}/tables/rename`,
      body: request,
      headers: { 'Idempotency-Key': generateIdempotencyKey() },
    })
  }

  async createTableIfNotExists(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    try {
      return await this.createTable(namespace, request)
    } catch (error) {
      if (error instanceof IcebergError && error.status === 409) {
        return await this.loadTable({ namespace: namespace.namespace, name: request.name })
      }
      throw error
    }
  }
}
