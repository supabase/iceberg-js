export { IcebergRestCatalog } from './catalog/IcebergRestCatalog'
export type { IcebergRestCatalogOptions, AccessDelegation } from './catalog/IcebergRestCatalog'

export type {
  NamespaceIdentifier,
  NamespaceMetadata,
  TableIdentifier,
  TableSchema,
  TableField,
  IcebergType,
  PartitionSpec,
  PartitionField,
  SortOrder,
  SortField,
  CreateTableRequest,
  UpdateTableRequest,
  DropTableRequest,
  TableMetadata,
} from './catalog/types'

export { getCurrentSchema } from './catalog/types'

export type { AuthConfig } from './http/types'

export { IcebergError } from './errors/IcebergError'
export type { IcebergErrorResponse } from './errors/IcebergError'
