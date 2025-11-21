// ESM Test - tests that the package works in ESM mode
import { IcebergRestCatalog } from 'iceberg-js'

console.log('✅ ESM: Successfully imported IcebergRestCatalog')
console.log('✅ ESM: Type:', typeof IcebergRestCatalog)
console.log('✅ ESM: Constructor name:', IcebergRestCatalog.name)

// Test instantiation
try {
  const catalog = new IcebergRestCatalog({
    baseUrl: 'http://localhost:8181',
    auth: { type: 'none' },
  })
  console.log('✅ ESM: Successfully created catalog instance')
  console.log('✅ ESM: ALL TESTS PASSED!')
} catch (error) {
  console.error('❌ ESM: Failed to create instance:', error)
  process.exit(1)
}
