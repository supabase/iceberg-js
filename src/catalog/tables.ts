import type { HttpClient } from '../http/types'
import { IcebergError } from '../errors/IcebergError'
import type {
  CreateTableRequest,
  CommitTableResponse,
  ListTablesResponse,
  LoadTableResponse,
  NamespaceIdentifier,
  TableIdentifier,
  TableMetadata,
  UpdateTableRequest,
  DropTableRequest,
} from './types'

function namespaceToPath(namespace: string[]): string {
  return namespace.join('\x1F')
}

export class TableOperations {
  constructor(
    private readonly client: HttpClient,
    private readonly prefix: string = '',
    private readonly accessDelegation?: string
  ) {}

  async listTables(namespace: NamespaceIdentifier): Promise<TableIdentifier[]> {
    const response = await this.client.request<ListTablesResponse>({
      method: 'GET',
      path: `${this.prefix}/namespaces/${namespaceToPath(namespace.namespace)}/tables`,
    })

    return response.data.identifiers
  }

  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    const headers: Record<string, string> = {}
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }

    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${this.prefix}/namespaces/${namespaceToPath(namespace.namespace)}/tables`,
      body: request,
      headers,
    })

    return response.data.metadata
  }

  async updateTable(
    id: TableIdentifier,
    request: UpdateTableRequest
  ): Promise<CommitTableResponse> {
    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${id.name}`,
      body: request,
    })

    return {
      'metadata-location': response.data['metadata-location'],
      metadata: response.data.metadata,
    }
  }

  async dropTable(id: TableIdentifier, options?: DropTableRequest): Promise<void> {
    await this.client.request<void>({
      method: 'DELETE',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${id.name}`,
      query: { purgeRequested: String(options?.purge ?? false) },
    })
  }

  async loadTable(id: TableIdentifier): Promise<TableMetadata> {
    const headers: Record<string, string> = {}
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }

    const response = await this.client.request<LoadTableResponse>({
      method: 'GET',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${id.name}`,
      headers,
    })

    return response.data.metadata
  }

  async tableExists(id: TableIdentifier): Promise<boolean> {
    const headers: Record<string, string> = {}
    if (this.accessDelegation) {
      headers['X-Iceberg-Access-Delegation'] = this.accessDelegation
    }

    try {
      await this.client.request<void>({
        method: 'HEAD',
        path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${id.name}`,
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
