import type { HttpClient } from '../http/types'
import type {
  CreateTableRequest,
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

  async updateTable(id: TableIdentifier, request: UpdateTableRequest): Promise<TableMetadata> {
    const response = await this.client.request<LoadTableResponse>({
      method: 'POST',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}/tables/${id.name}`,
      body: request,
    })

    return response.data.metadata
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
}
