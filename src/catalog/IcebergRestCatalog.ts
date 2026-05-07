import { createFetchClient } from '../http/createFetchClient'
import type { AuthConfig, HttpClient } from '../http/types'
import { NamespaceOperations } from './namespaces'
import { TableOperations } from './tables'
import type {
  CatalogConfig,
  CommitTableRequest,
  CommitTableResponse,
  CreateNamespaceResponse,
  CreateTableRequest,
  DropTableRequest,
  ListNamespacesOptions,
  ListNamespacesResult,
  ListTablesOptions,
  ListTablesResult,
  LoadTableOptions,
  LoadTableResultWithEtag,
  NamespaceIdentifier,
  NamespaceMetadata,
  TableIdentifier,
  TableMetadata,
  UpdateNamespacePropertiesRequest,
  UpdateNamespacePropertiesResponse,
  UpdateTableRequest,
} from './types'

/**
 * Access delegation mechanisms supported by the Iceberg REST Catalog.
 *
 * - `vended-credentials`: Server provides temporary credentials for data access
 * - `remote-signing`: Server signs requests on behalf of the client
 */
export type AccessDelegation = 'vended-credentials' | 'remote-signing'

/**
 * Configuration options for the Iceberg REST Catalog client.
 */
export interface IcebergRestCatalogOptions {
  /** Base URL of the Iceberg REST Catalog API */
  baseUrl: string
  /**
   * Warehouse identifier. The client passes this to `GET /v1/config?warehouse=…`
   * on first use; the server-returned `overrides.prefix` is then used for all
   * subsequent requests. This is the spec-recommended way to address a warehouse
   * (e.g., a Cloudflare R2 bucket) and replaces the older `catalogName` flow.
   */
  warehouse?: string
  /**
   * Alias for {@link warehouse} kept for backward compatibility. If both are
   * set, `warehouse` wins.
   */
  catalogName?: string
  /** Authentication configuration */
  auth?: AuthConfig
  /** Custom fetch implementation (defaults to globalThis.fetch) */
  fetch?: typeof fetch
  /**
   * Access delegation mechanisms to request from the server.
   * When specified, the X-Iceberg-Access-Delegation header will be sent
   * with supported operations (createTable, loadTable).
   *
   * @example ['vended-credentials']
   * @example ['vended-credentials', 'remote-signing']
   */
  accessDelegation?: AccessDelegation[]
}

/**
 * Client for interacting with an Apache Iceberg REST Catalog.
 *
 * This class provides methods for managing namespaces and tables in an Iceberg catalog.
 * It handles authentication, request formatting, and error handling automatically.
 *
 * @example
 * ```typescript
 * const catalog = new IcebergRestCatalog({
 *   baseUrl: 'https://my-catalog.example.com',
 *   warehouse: 'my-warehouse',
 *   auth: { type: 'bearer', token: process.env.ICEBERG_TOKEN }
 * });
 *
 * // First call lazily fetches /v1/config?warehouse=my-warehouse
 * await catalog.createNamespace({ namespace: ['analytics'] });
 * ```
 */
export class IcebergRestCatalog {
  private readonly client: HttpClient
  private readonly namespaceOps: NamespaceOperations
  private readonly tableOps: TableOperations
  private readonly accessDelegation?: string
  private readonly warehouse?: string

  private prefixPromise?: Promise<string>
  private cachedConfig?: CatalogConfig

  /**
   * Creates a new Iceberg REST Catalog client.
   *
   * @param options - Configuration options for the catalog client
   */
  constructor(options: IcebergRestCatalogOptions) {
    const baseUrl = options.baseUrl.endsWith('/') ? options.baseUrl : `${options.baseUrl}/`

    this.warehouse = options.warehouse ?? options.catalogName

    this.client = createFetchClient({
      baseUrl,
      auth: options.auth,
      fetchImpl: options.fetch,
    })

    // Format accessDelegation as comma-separated string per spec
    this.accessDelegation = options.accessDelegation?.join(',')

    const getPrefix = () => this.resolvePrefix()
    this.namespaceOps = new NamespaceOperations(this.client, getPrefix)
    this.tableOps = new TableOperations(this.client, getPrefix, this.accessDelegation)
  }

  /**
   * Fetch and cache the server's catalog configuration. Subsequent calls return
   * the same object. Calling this is optional — operations will trigger it
   * lazily on first use.
   */
  async loadConfig(): Promise<CatalogConfig> {
    if (this.cachedConfig) return this.cachedConfig
    const query: Record<string, string | undefined> = {}
    if (this.warehouse !== undefined) query.warehouse = this.warehouse
    const response = await this.client.request<CatalogConfig>({
      method: 'GET',
      path: 'v1/config',
      query: Object.keys(query).length ? query : undefined,
    })
    this.cachedConfig = response.data
    return response.data
  }

  private async resolvePrefix(): Promise<string> {
    if (!this.prefixPromise) {
      this.prefixPromise = this.computePrefix()
    }
    return this.prefixPromise
  }

  private async computePrefix(): Promise<string> {
    if (this.warehouse === undefined) {
      // No warehouse → no /config call. Use plain `v1` prefix.
      return 'v1'
    }
    try {
      const config = await this.loadConfig()
      const serverPrefix = config.overrides?.prefix ?? config.defaults?.prefix
      return serverPrefix ? `v1/${serverPrefix}` : `v1/${this.warehouse}`
    } catch {
      // /config is optional in some deployments; fall back to using the
      // warehouse as a literal path segment (matches the legacy catalogName
      // behavior).
      return `v1/${this.warehouse}`
    }
  }

  /**
   * Lists all namespaces in the catalog.
   *
   * @returns Paginated namespace list with optional `nextPageToken`
   *
   * @example
   * ```typescript
   * const { namespaces } = await catalog.listNamespaces();
   *
   * // List children under a parent
   * const { namespaces: children } = await catalog.listNamespaces({
   *   parent: { namespace: ['analytics'] },
   * });
   *
   * // Paginate
   * const page1 = await catalog.listNamespaces({ pageSize: 100 });
   * const page2 = await catalog.listNamespaces({ pageSize: 100, pageToken: page1.nextPageToken });
   * ```
   */
  async listNamespaces(options: ListNamespacesOptions = {}): Promise<ListNamespacesResult> {
    return this.namespaceOps.listNamespaces(options)
  }

  /**
   * Creates a new namespace in the catalog.
   */
  async createNamespace(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse> {
    return this.namespaceOps.createNamespace(id, metadata)
  }

  /**
   * Drops a namespace from the catalog.
   *
   * The namespace must be empty (contain no tables) before it can be dropped.
   */
  async dropNamespace(id: NamespaceIdentifier): Promise<void> {
    await this.namespaceOps.dropNamespace(id)
  }

  /**
   * Loads metadata for a namespace.
   */
  async loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata> {
    return this.namespaceOps.loadNamespaceMetadata(id)
  }

  /**
   * Set or remove properties on a namespace.
   */
  async updateNamespaceProperties(
    id: NamespaceIdentifier,
    request: UpdateNamespacePropertiesRequest
  ): Promise<UpdateNamespacePropertiesResponse> {
    return this.namespaceOps.updateNamespaceProperties(id, request)
  }

  /**
   * Lists tables in a namespace.
   *
   * @returns Paginated table list with optional `nextPageToken`
   */
  async listTables(
    namespace: NamespaceIdentifier,
    options: ListTablesOptions = {}
  ): Promise<ListTablesResult> {
    return this.tableOps.listTables(namespace, options)
  }

  /**
   * Creates a new table in the catalog.
   */
  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    return this.tableOps.createTable(namespace, request)
  }

  /**
   * Commit updates to a table using the spec-aligned `{ requirements, updates }` shape.
   *
   * @example
   * ```typescript
   * await catalog.updateTable(
   *   { namespace: ['analytics'], name: 'events' },
   *   {
   *     requirements: [],
   *     updates: [{ action: 'set-properties', updates: { 'read.split.target-size': '134217728' } }],
   *   },
   * );
   * ```
   */
  async updateTable(
    id: TableIdentifier,
    request: UpdateTableRequest
  ): Promise<CommitTableResponse> {
    return this.tableOps.updateTable(id, request)
  }

  /** Spec-aligned alias for {@link updateTable}. */
  async commitTable(
    id: TableIdentifier,
    request: CommitTableRequest
  ): Promise<CommitTableResponse> {
    return this.tableOps.commitTable(id, request)
  }

  /**
   * Drops a table from the catalog.
   */
  async dropTable(id: TableIdentifier, options?: DropTableRequest): Promise<void> {
    await this.tableOps.dropTable(id, options)
  }

  /**
   * Loads metadata for a table.
   *
   * Pass `ifNoneMatch` (a previous ETag) to perform a conditional GET. If the
   * server returns 304 Not Modified, the method returns `null`.
   */
  async loadTable(id: TableIdentifier): Promise<TableMetadata>
  async loadTable(id: TableIdentifier, options: LoadTableOptions): Promise<TableMetadata | null>
  async loadTable(id: TableIdentifier, options?: LoadTableOptions): Promise<TableMetadata | null> {
    return options ? this.tableOps.loadTable(id, options) : this.tableOps.loadTable(id)
  }

  /**
   * Spec-aligned `LoadTableResult` wrapper, including server `config`,
   * `storage-credentials`, and the response `ETag`. Returns `null` on 304.
   */
  async loadTableResult(
    id: TableIdentifier,
    options?: LoadTableOptions
  ): Promise<LoadTableResultWithEtag | null> {
    return this.tableOps.loadTableResult(id, options)
  }

  /**
   * Checks if a namespace exists in the catalog.
   */
  async namespaceExists(id: NamespaceIdentifier): Promise<boolean> {
    return this.namespaceOps.namespaceExists(id)
  }

  /**
   * Checks if a table exists in the catalog.
   */
  async tableExists(id: TableIdentifier): Promise<boolean> {
    return this.tableOps.tableExists(id)
  }

  /**
   * Creates a namespace if it does not exist.
   */
  async createNamespaceIfNotExists(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse | void> {
    return this.namespaceOps.createNamespaceIfNotExists(id, metadata)
  }

  /**
   * Creates a table if it does not exist.
   */
  async createTableIfNotExists(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    return this.tableOps.createTableIfNotExists(namespace, request)
  }
}
