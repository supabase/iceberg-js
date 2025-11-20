import { IcebergRestCatalog, IcebergError, getCurrentSchema } from '../../src/index'

async function testLocalCatalog() {
  console.log('🚀 Testing local Iceberg REST Catalog...\n')

  const catalog = new IcebergRestCatalog({
    baseUrl: 'http://localhost:8181',
    auth: { type: 'none' },
  })

  try {
    // 0. Cleanup: Drop test namespace if it exists (to make test idempotent)
    console.log('🧹 Cleaning up existing test data...')
    try {
      // Try to drop the table first (must drop table before namespace)
      await catalog.dropTable({ namespace: ['test'], name: 'users' })
      console.log('  Dropped existing table')
    } catch (error) {
      if (error instanceof IcebergError && error.status === 404) {
        console.log('  No existing table to clean up')
      } else {
        throw error
      }
    }

    try {
      await catalog.dropNamespace({ namespace: ['test'] })
      console.log('  Dropped existing namespace')
    } catch (error) {
      if (error instanceof IcebergError && error.status === 404) {
        console.log('  No existing namespace to clean up')
      } else {
        throw error
      }
    }
    console.log('✅ Cleanup complete!\n')

    // 1. List namespaces
    console.log('📋 Listing namespaces...')
    const namespaces = await catalog.listNamespaces()
    console.log('Namespaces:', namespaces)
    console.log('✅ Success!\n')

    // 2. Create a test namespace
    console.log('📁 Creating namespace "test"...')
    await catalog.createNamespace(
      { namespace: ['test'] },
      { properties: { owner: 'iceberg-js-test' } }
    )
    console.log('✅ Namespace created!\n')

    // 3. List namespaces again
    console.log('📋 Listing namespaces again...')
    const updatedNamespaces = await catalog.listNamespaces()
    console.log('Namespaces:', updatedNamespaces)
    console.log('✅ Success!\n')

    // 4. Create a simple table
    console.log('📊 Creating table "test.users"...')
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
    console.log('Table location:', tableMetadata.location)
    console.log('✅ Table created!\n')

    // 5. List tables
    console.log('📋 Listing tables in "test" namespace...')
    const tables = await catalog.listTables({ namespace: ['test'] })
    console.log('Tables:', tables)
    console.log('✅ Success!\n')

    // 6. Load table metadata
    console.log('📖 Loading table metadata...')
    const loadedTable = await catalog.loadTable({ namespace: ['test'], name: 'users' })
    const currentSchema = getCurrentSchema(loadedTable)
    console.log('Current schema:', currentSchema)
    console.log('All schemas:', loadedTable.schemas)
    console.log('✅ Success!\n')

    // 7. Update table properties
    console.log('✏️  Updating table properties...')
    await catalog.updateTable(
      { namespace: ['test'], name: 'users' },
      {
        properties: {
          'read.split.target-size': '134217728',
          'write.parquet.compression-codec': 'snappy',
        },
      }
    )
    console.log('✅ Properties updated!\n')

    console.log('✨ All tests passed!')
  } catch (error) {
    console.error('❌ Error:', error)
    process.exit(1)
  }
}

testLocalCatalog()
