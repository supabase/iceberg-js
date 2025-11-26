export interface NamespaceIdentifier {
  namespace: string[]
}

export interface NamespaceMetadata {
  properties: Record<string, string>
}

export interface TableIdentifier {
  namespace: string[]
  name: string
}

/**
 * Primitive types in Iceberg - all represented as strings.
 * Parameterized types use string format: decimal(precision,scale) and fixed[length]
 */
export type PrimitiveType =
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'string'
  | 'timestamp'
  | 'date'
  | 'time'
  | 'timestamptz'
  | 'uuid'
  | 'binary'
  | `decimal(${number},${number})`
  | `fixed[${number}]`

/**
 * Struct type - a nested structure containing fields.
 * Used for nested records within a field.
 */
export interface StructType {
  type: 'struct'
  fields: StructField[]
}

/**
 * List type - an array of elements.
 */
export interface ListType {
  type: 'list'
  'element-id': number
  element: IcebergType
  'element-required': boolean
}

/**
 * Map type - a key-value mapping.
 */
export interface MapType {
  type: 'map'
  'key-id': number
  key: IcebergType
  'value-id': number
  value: IcebergType
  'value-required': boolean
}

/**
 * Union of all Iceberg types.
 * Can be a primitive type (string) or a complex type (struct, list, map).
 */
export type IcebergType = PrimitiveType | StructType | ListType | MapType

/**
 * Primitive type values for default values.
 * Represents the possible values for initial-default and write-default.
 */
export type PrimitiveTypeValue = boolean | number | string

/**
 * A field within a struct (used in nested StructType).
 */
export interface StructField {
  id: number
  name: string
  type: IcebergType
  required: boolean
  doc?: string
  'initial-default'?: PrimitiveTypeValue
  'write-default'?: PrimitiveTypeValue
}

/**
 * A field within a table schema (top-level).
 * Equivalent to StructField but kept for backwards compatibility.
 */
export interface TableField {
  id: number
  name: string
  type: IcebergType
  required: boolean
  doc?: string
  'initial-default'?: PrimitiveTypeValue
  'write-default'?: PrimitiveTypeValue
}

export interface TableSchema {
  type: 'struct'
  fields: TableField[]
  'schema-id'?: number
  'identifier-field-ids'?: number[]
}

export interface PartitionField {
  source_id: number
  field_id: number
  name: string
  transform: string
}

export interface PartitionSpec {
  'spec-id': number
  fields: PartitionField[]
}

export interface SortField {
  source_id: number
  transform: string
  direction: 'asc' | 'desc'
  null_order: 'nulls-first' | 'nulls-last'
}

export interface SortOrder {
  'order-id': number
  fields: SortField[]
}

export interface CreateTableRequest {
  name: string
  schema: TableSchema
  'partition-spec'?: PartitionSpec
  'write-order'?: SortOrder
  properties?: Record<string, string>
  'stage-create'?: boolean
}

export interface UpdateTableRequest {
  schema?: TableSchema
  'partition-spec'?: PartitionSpec
  properties?: Record<string, string>
}

export interface DropTableRequest {
  purge?: boolean
}

export interface TableMetadata {
  name?: string
  location: string
  schemas: TableSchema[]
  'current-schema-id': number
  'partition-specs': PartitionSpec[]
  'default-spec-id'?: number
  'sort-orders': SortOrder[]
  'default-sort-order-id'?: number
  properties: Record<string, string>
  'metadata-location'?: string
  'current-snapshot-id'?: number
  snapshots?: unknown[]
  'snapshot-log'?: unknown[]
  'metadata-log'?: unknown[]
  refs?: Record<string, unknown>
  'last-updated-ms'?: number
  'last-column-id'?: number
  'last-sequence-number'?: number
  'table-uuid'?: string
  'format-version'?: number
  'last-partition-id'?: number
}

export interface CreateNamespaceRequest {
  namespace: string[]
  properties?: Record<string, string>
}

export interface CreateNamespaceResponse {
  namespace: string[]
  properties?: Record<string, string>
}

export interface GetNamespaceResponse {
  namespace: string[]
  properties: Record<string, string>
}

export interface ListNamespacesResponse {
  namespaces: string[][]
  'next-page-token'?: string
}

export interface ListTablesResponse {
  identifiers: TableIdentifier[]
  'next-page-token'?: string
}

export interface LoadTableResponse {
  'metadata-location': string
  metadata: TableMetadata
  config?: Record<string, string>
}

export interface CommitTableResponse {
  'metadata-location': string
  metadata: TableMetadata
}

/**
 * Gets the current (active) schema from table metadata.
 *
 * @param metadata - Table metadata containing schemas array and current-schema-id
 * @returns The current table schema, or undefined if not found
 */
export function getCurrentSchema(metadata: TableMetadata): TableSchema | undefined {
  return metadata.schemas.find((s) => s['schema-id'] === metadata['current-schema-id'])
}
