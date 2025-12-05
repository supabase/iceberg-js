import { createFetchClient } from '../http/createFetchClient'
import type { AuthConfig, HttpClient } from '../http/types'
import { NamespaceOperations } from './namespaces'
import { TableOperations } from './tables'
import type {
  CreateTableRequest,
  CreateNamespaceResponse,
  CommitTableResponse,
  NamespaceIdentifier,
  NamespaceMetadata,
  TableIdentifier,
  TableMetadata,
  UpdateTableRequest,
  DropTableRequest,
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
  /** Optional catalog name prefix for multi-catalog servers */
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
 *   baseUrl: 'https://my-catalog.example.com/iceberg/v1',
 *   auth: { type: 'bearer', token: process.env.ICEBERG_TOKEN }
 * });
 *
 * // Create a namespace
 * await catalog.createNamespace({ namespace: ['analytics'] });
 *
 * // Create a table
 * await catalog.createTable(
 *   { namespace: ['analytics'] },
 *   {
 *     name: 'events',
 *     schema: { type: 'struct', fields: [...] }
 *   }
 * );
 * ```
 */
export class IcebergRestCatalog {
  private readonly client: HttpClient
  private readonly namespaceOps: NamespaceOperations
  private readonly tableOps: TableOperations
  private readonly accessDelegation?: string

  /**
   * Creates a new Iceberg REST Catalog client.
   *
   * @param options - Configuration options for the catalog client
   */
  constructor(options: IcebergRestCatalogOptions) {
    let prefix = 'v1'
    if (options.catalogName) {
      prefix += `/${options.catalogName}`
    }

    const baseUrl = options.baseUrl.endsWith('/') ? options.baseUrl : `${options.baseUrl}/`

    this.client = createFetchClient({
      baseUrl,
      auth: options.auth,
      fetchImpl: options.fetch,
    })

    // Format accessDelegation as comma-separated string per spec
    this.accessDelegation = options.accessDelegation?.join(',')

    this.namespaceOps = new NamespaceOperations(this.client, prefix)
    this.tableOps = new TableOperations(this.client, prefix, this.accessDelegation)
  }

  /**
   * Lists all namespaces in the catalog.
   *
   * @param parent - Optional parent namespace to list children under
   * @returns Array of namespace identifiers
   *
   * @example
   * ```typescript
   * // List all top-level namespaces
   * const namespaces = await catalog.listNamespaces();
   *
   * // List namespaces under a parent
   * const children = await catalog.listNamespaces({ namespace: ['analytics'] });
   * ```
   */
  async listNamespaces(parent?: NamespaceIdentifier): Promise<NamespaceIdentifier[]> {
    return this.namespaceOps.listNamespaces(parent)
  }

  /**
   * Creates a new namespace in the catalog.
   *
   * @param id - Namespace identifier to create
   * @param metadata - Optional metadata properties for the namespace
   * @returns Response containing the created namespace and its properties
   *
   * @example
   * ```typescript
   * const response = await catalog.createNamespace(
   *   { namespace: ['analytics'] },
   *   { properties: { owner: 'data-team' } }
   * );
   * console.log(response.namespace); // ['analytics']
   * console.log(response.properties); // { owner: 'data-team', ... }
   * ```
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
   *
   * @param id - Namespace identifier to drop
   *
   * @example
   * ```typescript
   * await catalog.dropNamespace({ namespace: ['analytics'] });
   * ```
   */
  async dropNamespace(id: NamespaceIdentifier): Promise<void> {
    await this.namespaceOps.dropNamespace(id)
  }

  /**
   * Loads metadata for a namespace.
   *
   * @param id - Namespace identifier to load
   * @returns Namespace metadata including properties
   *
   * @example
   * ```typescript
   * const metadata = await catalog.loadNamespaceMetadata({ namespace: ['analytics'] });
   * console.log(metadata.properties);
   * ```
   */
  async loadNamespaceMetadata(id: NamespaceIdentifier): Promise<NamespaceMetadata> {
    return this.namespaceOps.loadNamespaceMetadata(id)
  }

  /**
   * Lists all tables in a namespace.
   *
   * @param namespace - Namespace identifier to list tables from
   * @returns Array of table identifiers
   *
   * @example
   * ```typescript
   * const tables = await catalog.listTables({ namespace: ['analytics'] });
   * console.log(tables); // [{ namespace: ['analytics'], name: 'events' }, ...]
   * ```
   */
  async listTables(namespace: NamespaceIdentifier): Promise<TableIdentifier[]> {
    return this.tableOps.listTables(namespace)
  }

  /**
   * Creates a new table in the catalog.
   *
   * @param namespace - Namespace to create the table in
   * @param request - Table creation request including name, schema, partition spec, etc.
   * @returns Table metadata for the created table
   *
   * @example
   * ```typescript
   * const metadata = await catalog.createTable(
   *   { namespace: ['analytics'] },
   *   {
   *     name: 'events',
   *     schema: {
   *       type: 'struct',
   *       fields: [
   *         { id: 1, name: 'id', type: 'long', required: true },
   *         { id: 2, name: 'timestamp', type: 'timestamp', required: true }
   *       ],
   *       'schema-id': 0
   *     },
   *     'partition-spec': {
   *       'spec-id': 0,
   *       fields: [
   *         { source_id: 2, field_id: 1000, name: 'ts_day', transform: 'day' }
   *       ]
   *     }
   *   }
   * );
   * ```
   */
  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    return this.tableOps.createTable(namespace, request)
  }

  /**
   * Updates an existing table's metadata.
   *
   * Can update the schema, partition spec, or properties of a table.
   *
   * @param id - Table identifier to update
   * @param request - Update request with fields to modify
   * @returns Response containing the metadata location and updated table metadata
   *
   * @example
   * ```typescript
   * const response = await catalog.updateTable(
   *   { namespace: ['analytics'], name: 'events' },
   *   {
   *     properties: { 'read.split.target-size': '134217728' }
   *   }
   * );
   * console.log(response['metadata-location']); // s3://...
   * console.log(response.metadata); // TableMetadata object
   * ```
   */
  async updateTable(
    id: TableIdentifier,
    request: UpdateTableRequest
  ): Promise<CommitTableResponse> {
    return this.tableOps.updateTable(id, request)
  }

  /**
   * Drops a table from the catalog.
   *
   * @param id - Table identifier to drop
   *
   * @example
   * ```typescript
   * await catalog.dropTable({ namespace: ['analytics'], name: 'events' });
   * ```
   */
  async dropTable(id: TableIdentifier, options?: DropTableRequest): Promise<void> {
    await this.tableOps.dropTable(id, options)
  }

  /**
   * Loads metadata for a table.
   *
   * @param id - Table identifier to load
   * @returns Table metadata including schema, partition spec, location, etc.
   *
   * @example
   * ```typescript
   * const metadata = await catalog.loadTable({ namespace: ['analytics'], name: 'events' });
   * console.log(metadata.schema);
   * console.log(metadata.location);
   * ```
   */
  async loadTable(id: TableIdentifier): Promise<TableMetadata> {
    return this.tableOps.loadTable(id)
  }

  /**
   * Checks if a namespace exists in the catalog.
   *
   * @param id - Namespace identifier to check
   * @returns True if the namespace exists, false otherwise
   *
   * @example
   * ```typescript
   * const exists = await catalog.namespaceExists({ namespace: ['analytics'] });
   * console.log(exists); // true or false
   * ```
   */
  async namespaceExists(id: NamespaceIdentifier): Promise<boolean> {
    return this.namespaceOps.namespaceExists(id)
  }

  /**
   * Checks if a table exists in the catalog.
   *
   * @param id - Table identifier to check
   * @returns True if the table exists, false otherwise
   *
   * @example
   * ```typescript
   * const exists = await catalog.tableExists({ namespace: ['analytics'], name: 'events' });
   * console.log(exists); // true or false
   * ```
   */
  async tableExists(id: TableIdentifier): Promise<boolean> {
    return this.tableOps.tableExists(id)
  }

  /**
   * Creates a namespace if it does not exist.
   *
   * If the namespace already exists, returns void. If created, returns the response.
   *
   * @param id - Namespace identifier to create
   * @param metadata - Optional metadata properties for the namespace
   * @returns Response containing the created namespace and its properties, or void if it already exists
   *
   * @example
   * ```typescript
   * const response = await catalog.createNamespaceIfNotExists(
   *   { namespace: ['analytics'] },
   *   { properties: { owner: 'data-team' } }
   * );
   * if (response) {
   *   console.log('Created:', response.namespace);
   * } else {
   *   console.log('Already exists');
   * }
   * ```
   */
  async createNamespaceIfNotExists(
    id: NamespaceIdentifier,
    metadata?: NamespaceMetadata
  ): Promise<CreateNamespaceResponse | void> {
    return this.namespaceOps.createNamespaceIfNotExists(id, metadata)
  }

  /**
   * Creates a table if it does not exist.
   *
   * If the table already exists, returns its metadata instead.
   *
   * @param namespace - Namespace to create the table in
   * @param request - Table creation request including name, schema, partition spec, etc.
   * @returns Table metadata for the created or existing table
   *
   * @example
   * ```typescript
   * const metadata = await catalog.createTableIfNotExists(
   *   { namespace: ['analytics'] },
   *   {
   *     name: 'events',
   *     schema: {
   *       type: 'struct',
   *       fields: [
   *         { id: 1, name: 'id', type: 'long', required: true },
   *         { id: 2, name: 'timestamp', type: 'timestamp', required: true }
   *       ],
   *       'schema-id': 0
   *     }
   *   }
   * );
   * ```
   */
  async createTableIfNotExists(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<TableMetadata> {
    return this.tableOps.createTableIfNotExists(namespace, request)
  }
}
