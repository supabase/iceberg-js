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
  'source-id': number
  'field-id'?: number
  name: string
  transform: string
}

export interface PartitionSpec {
  'spec-id'?: number
  fields: PartitionField[]
}

export interface SortField {
  'source-id': number
  transform: string
  direction: 'asc' | 'desc'
  'null-order': 'nulls-first' | 'nulls-last'
}

export interface SortOrder {
  'order-id': number
  fields: SortField[]
}

export interface SnapshotReference {
  type: 'tag' | 'branch'
  'snapshot-id': number
  'max-ref-age-ms'?: number
  'max-snapshot-age-ms'?: number
  'min-snapshots-to-keep'?: number
}

export interface Snapshot {
  'snapshot-id': number
  'parent-snapshot-id'?: number
  'sequence-number'?: number
  'timestamp-ms': number
  'manifest-list': string
  summary: { operation: 'append' | 'replace' | 'overwrite' | 'delete'; [key: string]: string }
  'schema-id'?: number
  'first-row-id'?: number
  'added-rows'?: number
}

export interface BlobMetadata {
  type: string
  'snapshot-id': number
  'sequence-number': number
  fields: number[]
  properties?: Record<string, string>
}

export interface StatisticsFile {
  'snapshot-id': number
  'statistics-path': string
  'file-size-in-bytes': number
  'file-footer-size-in-bytes': number
  'blob-metadata': BlobMetadata[]
}

export interface PartitionStatisticsFile {
  'snapshot-id': number
  'statistics-path': string
  'file-size-in-bytes': number
}

export interface EncryptedKey {
  'key-id': string
  'encrypted-key-metadata': string
  'encrypted-by-id'?: string
  properties?: Record<string, string>
}

export interface CreateTableRequest {
  name: string
  schema: TableSchema
  location?: string
  'partition-spec'?: PartitionSpec
  'write-order'?: SortOrder
  properties?: Record<string, string>
  'stage-create'?: boolean
}

export interface RegisterTableRequest {
  name: string
  'metadata-location': string
  overwrite?: boolean
}

export interface RenameTableRequest {
  source: TableIdentifier
  destination: TableIdentifier
}

/**
 * Spec-aligned table commit request shape: requirements + updates arrays.
 * Used by `updateTable` / `commitTable`.
 */
export interface CommitTableRequest {
  identifier?: TableIdentifier
  requirements?: TableRequirement[]
  updates: TableUpdate[]
}

/**
 * @deprecated Use `CommitTableRequest` (with `updates` and optional `requirements`) instead.
 * This alias is kept for callers migrating from 0.x; new code should use the spec-aligned shape.
 */
export type UpdateTableRequest = CommitTableRequest

/* ===== TableUpdate discriminated union (per OpenAPI spec) ===== */

export interface AssignUUIDUpdate {
  action: 'assign-uuid'
  uuid: string
}

export interface UpgradeFormatVersionUpdate {
  action: 'upgrade-format-version'
  'format-version': number
}

export interface AddSchemaUpdate {
  action: 'add-schema'
  schema: TableSchema
  /** @deprecated server-managed; included for backward compat with older catalogs */
  'last-column-id'?: number
}

export interface SetCurrentSchemaUpdate {
  action: 'set-current-schema'
  'schema-id': number
}

export interface AddPartitionSpecUpdate {
  action: 'add-spec'
  spec: PartitionSpec
}

export interface SetDefaultSpecUpdate {
  action: 'set-default-spec'
  'spec-id': number
}

export interface AddSortOrderUpdate {
  action: 'add-sort-order'
  'sort-order': SortOrder
}

export interface SetDefaultSortOrderUpdate {
  action: 'set-default-sort-order'
  'sort-order-id': number
}

export interface AddSnapshotUpdate {
  action: 'add-snapshot'
  snapshot: Snapshot
}

export interface SetSnapshotRefUpdate extends SnapshotReference {
  action: 'set-snapshot-ref'
  'ref-name': string
}

export interface RemoveSnapshotsUpdate {
  action: 'remove-snapshots'
  'snapshot-ids': number[]
}

export interface RemoveSnapshotRefUpdate {
  action: 'remove-snapshot-ref'
  'ref-name': string
}

export interface SetLocationUpdate {
  action: 'set-location'
  location: string
}

export interface SetPropertiesUpdate {
  action: 'set-properties'
  updates: Record<string, string>
}

export interface RemovePropertiesUpdate {
  action: 'remove-properties'
  removals: string[]
}

export interface SetStatisticsUpdate {
  action: 'set-statistics'
  statistics: StatisticsFile
  /** @deprecated derive from `statistics.snapshot-id` */
  'snapshot-id'?: number
}

export interface RemoveStatisticsUpdate {
  action: 'remove-statistics'
  'snapshot-id': number
}

export interface SetPartitionStatisticsUpdate {
  action: 'set-partition-statistics'
  'partition-statistics': PartitionStatisticsFile
}

export interface RemovePartitionStatisticsUpdate {
  action: 'remove-partition-statistics'
  'snapshot-id': number
}

export interface RemovePartitionSpecsUpdate {
  action: 'remove-partition-specs'
  'spec-ids': number[]
}

export interface RemoveSchemasUpdate {
  action: 'remove-schemas'
  'schema-ids': number[]
}

export interface AddEncryptionKeyUpdate {
  action: 'add-encryption-key'
  'encryption-key': EncryptedKey
}

export interface RemoveEncryptionKeyUpdate {
  action: 'remove-encryption-key'
  'key-id': string
}

export type TableUpdate =
  | AssignUUIDUpdate
  | UpgradeFormatVersionUpdate
  | AddSchemaUpdate
  | SetCurrentSchemaUpdate
  | AddPartitionSpecUpdate
  | SetDefaultSpecUpdate
  | AddSortOrderUpdate
  | SetDefaultSortOrderUpdate
  | AddSnapshotUpdate
  | SetSnapshotRefUpdate
  | RemoveSnapshotsUpdate
  | RemoveSnapshotRefUpdate
  | SetLocationUpdate
  | SetPropertiesUpdate
  | RemovePropertiesUpdate
  | SetStatisticsUpdate
  | RemoveStatisticsUpdate
  | SetPartitionStatisticsUpdate
  | RemovePartitionStatisticsUpdate
  | RemovePartitionSpecsUpdate
  | RemoveSchemasUpdate
  | AddEncryptionKeyUpdate
  | RemoveEncryptionKeyUpdate

/* ===== TableRequirement discriminated union (per OpenAPI spec) ===== */

export interface AssertCreate {
  type: 'assert-create'
}

export interface AssertTableUUID {
  type: 'assert-table-uuid'
  uuid: string
}

export interface AssertRefSnapshotId {
  type: 'assert-ref-snapshot-id'
  ref: string
  'snapshot-id': number | null
}

export interface AssertLastAssignedFieldId {
  type: 'assert-last-assigned-field-id'
  'last-assigned-field-id': number
}

export interface AssertCurrentSchemaId {
  type: 'assert-current-schema-id'
  'current-schema-id': number
}

export interface AssertLastAssignedPartitionId {
  type: 'assert-last-assigned-partition-id'
  'last-assigned-partition-id': number
}

export interface AssertDefaultSpecId {
  type: 'assert-default-spec-id'
  'default-spec-id': number
}

export interface AssertDefaultSortOrderId {
  type: 'assert-default-sort-order-id'
  'default-sort-order-id': number
}

export type TableRequirement =
  | AssertCreate
  | AssertTableUUID
  | AssertRefSnapshotId
  | AssertLastAssignedFieldId
  | AssertCurrentSchemaId
  | AssertLastAssignedPartitionId
  | AssertDefaultSpecId
  | AssertDefaultSortOrderId

export interface DropTableRequest {
  purge?: boolean
}

export interface TableMetadata {
  'format-version': number
  'table-uuid': string
  location?: string
  'last-updated-ms'?: number
  'last-column-id'?: number
  schemas: TableSchema[]
  'current-schema-id': number
  'partition-specs': PartitionSpec[]
  'default-spec-id'?: number
  'last-partition-id'?: number
  'sort-orders': SortOrder[]
  'default-sort-order-id'?: number
  properties: Record<string, string>
  'metadata-location'?: string
  'current-snapshot-id'?: number
  snapshots?: Snapshot[]
  'snapshot-log'?: { 'snapshot-id': number; 'timestamp-ms': number }[]
  'metadata-log'?: { 'metadata-file': string; 'timestamp-ms': number }[]
  refs?: Record<string, SnapshotReference>
  'last-sequence-number'?: number
  'next-row-id'?: number
  statistics?: StatisticsFile[]
  'partition-statistics'?: PartitionStatisticsFile[]
  'encryption-keys'?: EncryptedKey[]
  /** Optional name field returned by some catalogs */
  name?: string
}

export interface StorageCredential {
  prefix: string
  config: Record<string, string>
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
  properties?: Record<string, string> | null
}

export interface UpdateNamespacePropertiesRequest {
  removals?: string[]
  updates?: Record<string, string>
}

export interface UpdateNamespacePropertiesResponse {
  updated: string[]
  removed: string[]
  missing?: string[] | null
}

export interface ListNamespacesResponse {
  namespaces?: string[][]
  'next-page-token'?: string | null
}

export interface ListTablesResponse {
  identifiers?: TableIdentifier[]
  'next-page-token'?: string | null
}

/**
 * Spec-aligned LoadTableResult (named LoadTableResponse here for backward compat).
 * Returned by GET /tables/{t}, POST /tables, and POST /tables/{t} (commit).
 */
export interface LoadTableResponse {
  metadata: TableMetadata
  'metadata-location'?: string
  config?: Record<string, string>
  'storage-credentials'?: StorageCredential[]
}

export type LoadTableResult = LoadTableResponse

export interface CommitTableResponse {
  'metadata-location': string
  metadata: TableMetadata
}

/**
 * Catalog configuration as returned by GET /v1/config?warehouse=...
 *
 * Servers can use `overrides.prefix` to direct subsequent requests to a
 * server-specific path prefix (e.g., the warehouse identifier), which is
 * the recommended pattern over hard-coded `catalogName` configuration.
 */
export interface CatalogConfig {
  defaults: Record<string, string>
  overrides: Record<string, string>
  endpoints?: string[]
  'idempotency-key-lifetime'?: string
}

/* ===== Method options ===== */

export interface ListNamespacesOptions {
  parent?: NamespaceIdentifier
  pageToken?: string
  pageSize?: number
}

export interface ListTablesOptions {
  pageToken?: string
  pageSize?: number
}

export interface ListNamespacesResult {
  namespaces: NamespaceIdentifier[]
  nextPageToken?: string
}

export interface ListTablesResult {
  identifiers: TableIdentifier[]
  nextPageToken?: string
}

export interface LoadTableOptions {
  /** ETag value from a previous response; server returns 304 (and we return null) if unchanged. */
  ifNoneMatch?: string
}

export interface LoadTableResultWithEtag extends LoadTableResponse {
  etag: string | null
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
