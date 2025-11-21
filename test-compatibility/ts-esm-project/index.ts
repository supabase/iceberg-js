// TypeScript ESM Test - tests that the package works in TS ESM mode with types
import { IcebergRestCatalog, type IcebergConfig } from 'iceberg-js'

console.log('✅ TS-ESM: Successfully imported IcebergRestCatalog and types')

// Test type checking
const config: IcebergConfig = {
  baseUrl: 'http://localhost:8181',
  auth: { type: 'none' },
}

// Test instantiation
try {
  const catalog = new IcebergRestCatalog(config)
  console.log('✅ TS-ESM: Successfully created catalog instance')
  console.log('✅ TS-ESM: Type inference working:', typeof catalog.listNamespaces === 'function')
  console.log('✅ TS-ESM: ALL TESTS PASSED!')
} catch (error) {
  console.error('❌ TS-ESM: Failed:', error)
  process.exit(1)
}
