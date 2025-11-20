import { IcebergRestCatalog, getCurrentSchema } from '../src/index'

async function main() {
  const catalog = new IcebergRestCatalog({
    baseUrl: 'https://my-catalog.example.com/iceberg/v1',
    auth: {
      type: 'bearer',
      token: 'your-token-here',
    },
  })

  try {
    // List namespaces
    const namespaces = await catalog.listNamespaces()
    console.log('Namespaces:', namespaces)

    // Create namespace
    await catalog.createNamespace(
      { namespace: ['analytics'] },
      { properties: { owner: 'data-team' } }
    )

    // List tables
    const tables = await catalog.listTables({ namespace: ['analytics'] })
    console.log('Tables:', tables)

    // Create table
    const metadata = await catalog.createTable(
      { namespace: ['analytics'] },
      {
        name: 'events',
        schema: {
          type: 'struct',
          fields: [
            { id: 1, name: 'id', type: 'long', required: true },
            { id: 2, name: 'timestamp', type: 'timestamp', required: true },
            { id: 3, name: 'user_id', type: 'string', required: false },
          ],
          'schema-id': 0,
          'identifier-field-ids': [1],
        },
        'partition-spec': {
          'spec-id': 0,
          fields: [
            {
              source_id: 2,
              field_id: 1000,
              name: 'ts_day',
              transform: 'day',
            },
          ],
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

    console.log('Created table:', metadata.name)

    // Load table
    const loadedTable = await catalog.loadTable({
      namespace: ['analytics'],
      name: 'events',
    })
    console.log('Loaded table location:', loadedTable.location)

    // Get the current schema
    const schema = getCurrentSchema(loadedTable)
    console.log('Current schema:', schema)
  } catch (error) {
    console.error('Error:', error)
  }
}

main()
