export { IcebergRestCatalog } from './catalog/IcebergRestCatalog'
export type { IcebergRestCatalogOptions, AccessDelegation } from './catalog/IcebergRestCatalog'

export type {
  // Identifiers
  NamespaceIdentifier,
  NamespaceMetadata,
  TableIdentifier,

  // Schema / type system
  TableSchema,
  TableField,
  StructField,
  IcebergType,
  PrimitiveType,
  StructType,
  ListType,
  MapType,
  PrimitiveTypeValue,

  // Partitioning / sort
  PartitionSpec,
  PartitionField,
  SortOrder,
  SortField,

  // Snapshots / metadata
  Snapshot,
  SnapshotReference,
  StatisticsFile,
  PartitionStatisticsFile,
  BlobMetadata,
  EncryptedKey,
  TableMetadata,

  // Requests / responses
  CreateTableRequest,
  RegisterTableRequest,
  RenameTableRequest,
  CommitTableRequest,
  CommitTableResponse,
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  GetNamespaceResponse,
  UpdateNamespacePropertiesRequest,
  UpdateNamespacePropertiesResponse,
  UpdateTableRequest,
  DropTableRequest,
  LoadTableResponse,
  LoadTableResult,
  LoadTableResultWithEtag,
  ListNamespacesResponse,
  ListTablesResponse,
  ListNamespacesOptions,
  ListNamespacesResult,
  ListTablesOptions,
  ListTablesResult,
  LoadTableOptions,

  // Catalog config
  CatalogConfig,
  StorageCredential,

  // Table updates / requirements
  TableUpdate,
  TableRequirement,
  AssignUUIDUpdate,
  UpgradeFormatVersionUpdate,
  AddSchemaUpdate,
  SetCurrentSchemaUpdate,
  AddPartitionSpecUpdate,
  SetDefaultSpecUpdate,
  AddSortOrderUpdate,
  SetDefaultSortOrderUpdate,
  AddSnapshotUpdate,
  SetSnapshotRefUpdate,
  RemoveSnapshotsUpdate,
  RemoveSnapshotRefUpdate,
  SetLocationUpdate,
  SetPropertiesUpdate,
  RemovePropertiesUpdate,
  SetStatisticsUpdate,
  RemoveStatisticsUpdate,
  SetPartitionStatisticsUpdate,
  RemovePartitionStatisticsUpdate,
  RemovePartitionSpecsUpdate,
  RemoveSchemasUpdate,
  AddEncryptionKeyUpdate,
  RemoveEncryptionKeyUpdate,
  AssertCreate,
  AssertTableUUID,
  AssertRefSnapshotId,
  AssertLastAssignedFieldId,
  AssertCurrentSchemaId,
  AssertLastAssignedPartitionId,
  AssertDefaultSpecId,
  AssertDefaultSortOrderId,
} from './catalog/types'

export {
  getCurrentSchema,
  parseDecimalType,
  parseFixedType,
  isDecimalType,
  isFixedType,
  typesEqual,
} from './catalog/types'

export type { AuthConfig } from './http/types'

export { IcebergError } from './errors/IcebergError'
export type { IcebergErrorResponse } from './errors/IcebergError'

export { generateIdempotencyKey } from './utils/idempotency-key'

export { ICEBERG_REST_SPEC_TAG, ICEBERG_REST_SPEC_URL } from './spec-version'
