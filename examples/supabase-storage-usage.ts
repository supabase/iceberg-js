import { IcebergRestCatalog } from '../src/index'

/**
 * Example: Using iceberg-js with Supabase Storage Analytics
 *
 * This demonstrates how to create tables in Supabase's Iceberg REST Catalog
 * using the iceberg-js library.
 */
async function main() {
  // Initialize the catalog client with Supabase Storage endpoint
  const catalog = new IcebergRestCatalog({
    baseUrl: 'your-catalog-url',
    auth: {
      type: 'bearer',
      token: process.env.SUPABASE_TOKEN || 'your-token-here',
    },
    // Request vended credentials from Supabase Storage
    // This allows the server to provide temporary AWS credentials
    // for accessing table data files in S3
    accessDelegation: ['vended-credentials'],
  })

  try {
    // Ensure the default namespace exists
    try {
      await catalog.createNamespace(
        { namespace: ['default'] },
        { properties: { description: 'Default namespace for analytics tables' } }
      )
      console.log('✓ Created default namespace')
    } catch (err) {
      if (err instanceof Error && err.message.includes('already exists')) {
        console.log('✓ Default namespace already exists')
      } else {
        throw err
      }
    }

    // Create an analytics table
    const tableMetadata = await catalog.createTable(
      { namespace: ['default'] },
      {
        name: 'analytics_events',
        schema: {
          type: 'struct',
          fields: [
            { id: 1, name: 'id', type: 'long', required: true },
            { id: 2, name: 'event_name', type: 'string', required: true },
            { id: 3, name: 'timestamp', type: 'timestamp', required: true },
            { id: 4, name: 'user_id', type: 'string', required: false },
            { id: 5, name: 'properties', type: 'string', required: false },
          ],
          'schema-id': 0,
          'identifier-field-ids': [1],
        },
        'partition-spec': {
          'spec-id': 0,
          fields: [
            {
              source_id: 3, // timestamp field
              field_id: 1000,
              name: 'event_day',
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
          'write.parquet.compression-codec': 'snappy',
        },
      }
    )

    console.log('✓ Created table:', tableMetadata.name)
    console.log('  Location:', tableMetadata.location)
    console.log('  Schema ID:', tableMetadata['current-schema-id'])

    // List all tables in the namespace
    const tables = await catalog.listTables({ namespace: ['default'] })
    console.log(
      '✓ Tables in default namespace:',
      tables.map((t) => t.name)
    )

    // Load the table metadata
    const loadedTable = await catalog.loadTable({
      namespace: ['default'],
      name: 'analytics_events',
    })
    console.log('✓ Loaded table location:', loadedTable.location)
  } catch (error) {
    console.error('Error:', error)
    process.exit(1)
  }
}

main()
