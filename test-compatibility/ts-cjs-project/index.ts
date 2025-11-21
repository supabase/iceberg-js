// TypeScript CJS Test - tests that the package works in TS CJS mode with types
import { IcebergRestCatalog, type IcebergConfig } from 'iceberg-js'

console.log('✅ TS-CJS: Successfully imported IcebergRestCatalog and types')

// Test type checking
const config: IcebergConfig = {
  baseUrl: 'http://localhost:8181',
  auth: { type: 'none' },
}

// Test instantiation
try {
  const catalog = new IcebergRestCatalog(config)
  console.log('✅ TS-CJS: Successfully created catalog instance')
  console.log('✅ TS-CJS: Type inference working:', typeof catalog.listNamespaces === 'function')
  console.log('✅ TS-CJS: ALL TESTS PASSED!')
} catch (error) {
  console.error('❌ TS-CJS: Failed:', error)
  process.exit(1)
}
