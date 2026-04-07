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
 *
 * Note: The OpenAPI spec defines PrimitiveType as `type: string`, so any string is valid.
 * We include known types for autocomplete, plus a catch-all for flexibility.
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
  | (string & {}) // catch-all for any format (e.g., "decimal(10, 2)" with spaces) and future types

/**
 * Regex patterns for parsing parameterized types.
 * These allow flexible whitespace matching.
 */
const DECIMAL_REGEX = /^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$/
const FIXED_REGEX = /^fixed\s*\[\s*(\d+)\s*\]$/

/**
 * Parse a decimal type string into its components.
 * Handles any whitespace formatting (e.g., "decimal(10,2)", "decimal(10, 2)", "decimal( 10 , 2 )").
 *
 * @param type - The type string to parse
 * @returns Object with precision and scale, or null if not a valid decimal type
 */
export function parseDecimalType(type: string): { precision: number; scale: number } | null {
  const match = type.match(DECIMAL_REGEX)
  if (!match) return null
  return {
    precision: parseInt(match[1], 10),
    scale: parseInt(match[2], 10),
  }
}

/**
 * Parse a fixed type string into its length.
 * Handles any whitespace formatting (e.g., "fixed[16]", "fixed[ 16 ]").
 *
 * @param type - The type string to parse
 * @returns Object with length, or null if not a valid fixed type
 */
export function parseFixedType(type: string): { length: number } | null {
  const match = type.match(FIXED_REGEX)
  if (!match) return null
  return {
    length: parseInt(match[1], 10),
  }
}

/**
 * Check if a type string is a decimal type.
 */
export function isDecimalType(type: string): boolean {
  return DECIMAL_REGEX.test(type)
}

/**
 * Check if a type string is a fixed type.
 */
export function isFixedType(type: string): boolean {
  return FIXED_REGEX.test(type)
}

/**
 * Compare two Iceberg type strings for equality, ignoring whitespace differences.
 * This is useful when comparing types from user input vs catalog responses,
 * as catalogs may normalize whitespace differently.
 *
 * @param a - First type string
 * @param b - Second type string
 * @returns true if the types are equivalent
 */
export function typesEqual(a: string, b: string): boolean {
  // For decimal types, compare parsed values
  const decimalA = parseDecimalType(a)
  const decimalB = parseDecimalType(b)
  if (decimalA && decimalB) {
    return decimalA.precision === decimalB.precision && decimalA.scale === decimalB.scale
  }

  // For fixed types, compare parsed values
  const fixedA = parseFixedType(a)
  const fixedB = parseFixedType(b)
  if (fixedA && fixedB) {
    return fixedA.length === fixedB.length
  }

  // For other types, direct string comparison
  return a === b
}

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
 * Response from the catalog configuration endpoint (GET /v1/config).
 */
export interface CatalogConfig {
  defaults: Record<string, string>
  overrides: Record<string, string>
  endpoints?: string[]
  'idempotency-key-lifetime'?: string
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
