// CommonJS Test - tests that the package works in CJS mode
const { IcebergRestCatalog } = require('iceberg-js')

console.log('✅ CJS: Successfully required IcebergRestCatalog')
console.log('✅ CJS: Type:', typeof IcebergRestCatalog)
console.log('✅ CJS: Constructor name:', IcebergRestCatalog.name)

// Test instantiation
try {
  const catalog = new IcebergRestCatalog({
    baseUrl: 'http://localhost:8181',
    auth: { type: 'none' },
  })
  console.log('✅ CJS: Successfully created catalog instance')
  console.log('✅ CJS: ALL TESTS PASSED!')
} catch (error) {
  console.error('❌ CJS: Failed to create instance:', error)
  process.exit(1)
}
