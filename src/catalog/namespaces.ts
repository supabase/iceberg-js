import type { HttpClient } from '../http/types'
import { IcebergError } from '../errors/IcebergError'
import type {
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  GetNamespaceResponse,
  ListNamespacesResponse,
  NamespaceIdentifier,
  NamespaceMetadata,
} from './types'

function constructPath(...parts: string[]): string {
  return parts.filter((p) => p).join('/')
}

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
      path: constructPath('v1', this.prefix, 'namespaces'),
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
      path: constructPath('v1', this.prefix, 'namespaces'),
      body: request,
    })

    return response.data
  }

  async dropNamespace(id: NamespaceIdentifier): Promise<void> {
    await this.client.request<void>({
      method: 'DELETE',
      path: constructPath('v1', this.prefix, 'namespaces', namespaceToPath(id.namespace)),
    })
  }

  async loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata> {
    const response = await this.client.request<GetNamespaceResponse>({
      method: 'GET',
      path: constructPath('v1', this.prefix, 'namespaces', namespaceToPath(id.namespace)),
    })

    return {
      properties: response.data.properties,
    }
  }

  async namespaceExists(id: NamespaceIdentifier): Promise<boolean> {
    try {
      await this.client.request<void>({
        method: 'HEAD',
        path: constructPath('v1', this.prefix, 'namespaces', namespaceToPath(id.namespace)),
      })
      return true
    } catch (error) {
      if (error instanceof IcebergError && error.status === 404) {
        return false
      }
      throw error
    }
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
