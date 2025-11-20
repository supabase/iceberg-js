import type { HttpClient } from '../http/types'
import type {
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  GetNamespaceResponse,
  ListNamespacesResponse,
  NamespaceIdentifier,
  NamespaceMetadata,
} from './types'

function namespaceToPath(namespace: string[]): string {
  return namespace.join('\x1F')
}

export class NamespaceOperations {
  constructor(
    private readonly client: HttpClient,
    private readonly prefix: string = ''
  ) {}

  async listNamespaces(parent?: NamespaceIdentifier): Promise<NamespaceIdentifier[]> {
    const query = parent ? { parent: namespaceToPath(parent.namespace) } : undefined

    const response = await this.client.request<ListNamespacesResponse>({
      method: 'GET',
      path: `${this.prefix}/namespaces`,
      query,
    })

    return response.data.namespaces.map((ns) => ({ namespace: ns }))
  }

  async createNamespace(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse> {
    const request: CreateNamespaceRequest = {
      namespace: id.namespace,
      properties: metadata?.properties,
    }

    const response = await this.client.request<CreateNamespaceResponse>({
      method: 'POST',
      path: `${this.prefix}/namespaces`,
      body: request,
    })

    return response.data
  }

  async dropNamespace(id: NamespaceIdentifier): Promise<void> {
    await this.client.request<void>({
      method: 'DELETE',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}`,
    })
  }

  async loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata> {
    const response = await this.client.request<GetNamespaceResponse>({
      method: 'GET',
      path: `${this.prefix}/namespaces/${namespaceToPath(id.namespace)}`,
    })

    return {
      properties: response.data.properties,
    }
  }
}
