export { IcebergRestCatalog } from './catalog/IcebergRestCatalog'
export type { IcebergRestCatalogOptions, AccessDelegation } from './catalog/IcebergRestCatalog'

export type {
  NamespaceIdentifier,
  NamespaceMetadata,
  TableIdentifier,
  TableSchema,
  TableField,
  StructField,
  IcebergType,
  PrimitiveType,
  StructType,
  ListType,
  MapType,
  PrimitiveTypeValue,
  PartitionSpec,
  PartitionField,
  SortOrder,
  SortField,
  CreateTableRequest,
  CreateNamespaceResponse,
  CommitTableResponse,
  UpdateTableRequest,
  DropTableRequest,
  TableMetadata,
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
