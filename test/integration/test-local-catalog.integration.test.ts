import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import {
  IcebergRestCatalog,
  IcebergError,
  getCurrentSchema,
  typesEqual,
  isDecimalType,
  isFixedType,
} from '../../src/index'

describe('Local Iceberg REST Catalog Integration', () => {
  const catalog = new IcebergRestCatalog({
    baseUrl: 'http://localhost:8181',
    auth: { type: 'none' },
  })

  beforeAll(async () => {
    // Cleanup: Drop test namespace if it exists (to make test idempotent)
    try {
      await catalog.dropTable({ namespace: ['test'], name: 'users' })
    } catch (error) {
      if (!(error instanceof IcebergError && error.status === 404)) {
        throw error
      }
    }

    try {
      await catalog.dropTable({ namespace: ['test'], name: 'complex_types' })
    } catch (error) {
      if (!(error instanceof IcebergError && error.status === 404)) {
        throw error
      }
    }

    try {
      await catalog.dropNamespace({ namespace: ['test'] })
    } catch (error) {
      if (!(error instanceof IcebergError && error.status === 404)) {
        throw error
      }
    }
  })

  afterAll(async () => {
    // Cleanup after all tests
    try {
      await catalog.dropTable({ namespace: ['test'], name: 'users' })
    } catch {
      // ignore
    }
    try {
      await catalog.dropTable({ namespace: ['test'], name: 'complex_types' })
    } catch {
      // ignore
    }
    try {
      await catalog.dropNamespace({ namespace: ['test'] })
    } catch {
      // ignore
    }
  })

  describe('Namespace Operations', () => {
    it('should list namespaces', async () => {
      const namespaces = await catalog.listNamespaces()
      expect(Array.isArray(namespaces)).toBe(true)
    })

    it('should create a namespace', async () => {
      await catalog.createNamespace(
        { namespace: ['test'] },
        { properties: { owner: 'iceberg-js-test' } }
      )

      const namespaces = await catalog.listNamespaces()
      const testNs = namespaces.find((ns) => ns.namespace[0] === 'test')
      expect(testNs).toBeDefined()
    })
  })

  describe('Table Operations', () => {
    it('should create a table', async () => {
      const tableMetadata = await catalog.createTable(
        { namespace: ['test'] },
        {
          name: 'users',
          schema: {
            type: 'struct',
            fields: [
              { id: 1, name: 'id', type: 'long', required: true },
              { id: 2, name: 'name', type: 'string', required: true },
              { id: 3, name: 'email', type: 'string', required: false },
            ],
            'schema-id': 0,
            'identifier-field-ids': [1],
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [],
          },
          'write-order': {
            'order-id': 0,
            fields: [],
          },
          properties: {
            'write.format.default': 'parquet',
          },
        }
      )

      expect(tableMetadata.location).toBeDefined()
      expect(tableMetadata.location).toContain('users')
    })

    it('should list tables in namespace', async () => {
      const tables = await catalog.listTables({ namespace: ['test'] })

      expect(Array.isArray(tables)).toBe(true)
      const usersTable = tables.find((t) => t.name === 'users')
      expect(usersTable).toBeDefined()
    })

    it('should load table metadata', async () => {
      const loadedTable = await catalog.loadTable({ namespace: ['test'], name: 'users' })

      expect(loadedTable.schemas).toBeDefined()
      expect(loadedTable['current-schema-id']).toBeDefined()

      const currentSchema = getCurrentSchema(loadedTable)
      expect(currentSchema).toBeDefined()
      expect(currentSchema?.fields).toHaveLength(3)
      expect(currentSchema?.fields[0].name).toBe('id')
    })

    it('should update table properties', async () => {
      const result = await catalog.updateTable(
        { namespace: ['test'], name: 'users' },
        {
          properties: {
            'read.split.target-size': '134217728',
            'write.parquet.compression-codec': 'snappy',
          },
        }
      )

      expect(result.metadata).toBeDefined()
    })
  })

  describe('Complex Types', () => {
    it('should create table with decimal, fixed, list, map, and struct types', async () => {
      const tableMetadata = await catalog.createTable(
        { namespace: ['test'] },
        {
          name: 'complex_types',
          schema: {
            type: 'struct',
            fields: [
              { id: 1, name: 'id', type: 'long', required: true },
              // Decimal type - string format
              { id: 2, name: 'price', type: 'decimal(10,2)', required: false },
              // Fixed type - string format
              { id: 3, name: 'hash', type: 'fixed[16]', required: false },
              // List type
              {
                id: 4,
                name: 'tags',
                type: {
                  type: 'list',
                  'element-id': 5,
                  element: 'string',
                  'element-required': false,
                },
                required: false,
              },
              // Map type
              {
                id: 6,
                name: 'metadata',
                type: {
                  type: 'map',
                  'key-id': 7,
                  key: 'string',
                  'value-id': 8,
                  value: 'string',
                  'value-required': false,
                },
                required: false,
              },
              // Nested struct type
              {
                id: 9,
                name: 'address',
                type: {
                  type: 'struct',
                  fields: [
                    { id: 10, name: 'street', type: 'string', required: false },
                    { id: 11, name: 'city', type: 'string', required: false },
                    { id: 12, name: 'zip', type: 'string', required: false },
                  ],
                },
                required: false,
              },
              // List of structs
              {
                id: 13,
                name: 'contacts',
                type: {
                  type: 'list',
                  'element-id': 14,
                  element: {
                    type: 'struct',
                    fields: [
                      { id: 15, name: 'name', type: 'string', required: true },
                      { id: 16, name: 'phone', type: 'string', required: false },
                    ],
                  },
                  'element-required': false,
                },
                required: false,
              },
            ],
            'schema-id': 0,
            'identifier-field-ids': [1],
          },
          'partition-spec': {
            'spec-id': 0,
            fields: [],
          },
          'write-order': {
            'order-id': 0,
            fields: [],
          },
          properties: {},
        }
      )

      expect(tableMetadata.location).toBeDefined()
    })

    it('should load complex types table and verify schema', async () => {
      const loadedTable = await catalog.loadTable({
        namespace: ['test'],
        name: 'complex_types',
      })

      const schema = getCurrentSchema(loadedTable)
      expect(schema).toBeDefined()

      const fields = schema!.fields

      // Use typesEqual for decimal/fixed to handle whitespace differences
      // Catalog may return "decimal(10, 2)" with space even if we sent "decimal(10,2)"
      const priceType = fields.find((f) => f.name === 'price')?.type as string
      expect(isDecimalType(priceType)).toBe(true)
      expect(typesEqual(priceType, 'decimal(10,2)')).toBe(true)

      const hashType = fields.find((f) => f.name === 'hash')?.type as string
      expect(isFixedType(hashType)).toBe(true)
      expect(typesEqual(hashType, 'fixed[16]')).toBe(true)

      const tagsField = fields.find((f) => f.name === 'tags')
      expect(tagsField?.type).toMatchObject({ type: 'list' })

      const metadataField = fields.find((f) => f.name === 'metadata')
      expect(metadataField?.type).toMatchObject({ type: 'map' })

      const addressField = fields.find((f) => f.name === 'address')
      expect(addressField?.type).toMatchObject({ type: 'struct' })
    })
  })

  describe('Invalid Types (Error Handling)', () => {
    // Helper to attempt creating a table with invalid fields
    async function createTableWithFields(
      tableName: string,
      fields: Array<{ id: number; name: string; type: unknown; required: boolean }>
    ) {
      return catalog.createTable(
        { namespace: ['test'] },
        {
          name: tableName,
          schema: {
            type: 'struct',
            fields: fields as never, // cast to bypass type checking for invalid type tests
            'schema-id': 0,
            'identifier-field-ids': [1],
          },
          'partition-spec': { 'spec-id': 0, fields: [] },
          'write-order': { 'order-id': 0, fields: [] },
          properties: {},
        }
      )
    }

    it('should reject invalid primitive type', async () => {
      await expect(
        createTableWithFields(`invalid_prim_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_field', type: 'invalid_type', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject decimal without params', async () => {
      await expect(
        createTableWithFields(`invalid_dec1_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_decimal', type: 'decimal', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject decimal with empty params', async () => {
      await expect(
        createTableWithFields(`invalid_dec2_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_decimal', type: 'decimal()', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject decimal missing scale', async () => {
      await expect(
        createTableWithFields(`invalid_dec3_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_decimal', type: 'decimal(10)', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject decimal with non-numeric params', async () => {
      await expect(
        createTableWithFields(`invalid_dec4_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_decimal', type: 'decimal(a,b)', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject fixed without length', async () => {
      await expect(
        createTableWithFields(`invalid_fix1_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_fixed', type: 'fixed', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject fixed with empty length', async () => {
      await expect(
        createTableWithFields(`invalid_fix2_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_fixed', type: 'fixed[]', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject fixed with non-numeric length', async () => {
      await expect(
        createTableWithFields(`invalid_fix3_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          { id: 2, name: 'bad_fixed', type: 'fixed[abc]', required: false },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject list missing element-id', async () => {
      await expect(
        createTableWithFields(`invalid_list_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          {
            id: 2,
            name: 'bad_list',
            type: {
              type: 'list',
              element: 'string',
              'element-required': false,
            },
            required: false,
          },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject map missing key-id', async () => {
      await expect(
        createTableWithFields(`invalid_map1_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          {
            id: 2,
            name: 'bad_map',
            type: {
              type: 'map',
              key: 'string',
              'value-id': 3,
              value: 'string',
              'value-required': false,
            },
            required: false,
          },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject map missing value-id', async () => {
      await expect(
        createTableWithFields(`invalid_map2_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          {
            id: 2,
            name: 'bad_map',
            type: {
              type: 'map',
              'key-id': 3,
              key: 'string',
              value: 'string',
              'value-required': false,
            },
            required: false,
          },
        ])
      ).rejects.toThrow(IcebergError)
    })

    it('should reject object type instead of string', async () => {
      // It must be a string: 'decimal(10,2)'
      // This test verifies the catalog rejects object format
      await expect(
        createTableWithFields(`invalid_obj_${Date.now()}`, [
          { id: 1, name: 'id', type: 'long', required: true },
          {
            id: 2,
            name: 'bad_decimal',
            type: { type: 'decimal', precision: 10, scale: 2 }, // object instead of string
            required: false,
          },
        ])
      ).rejects.toThrow(IcebergError)
    })
  })
})
