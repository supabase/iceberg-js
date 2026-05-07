// Resolves the Iceberg REST Catalog OpenAPI YAML for build- and test-time use.
//
// The client doesn't ship a copy of the YAML. Instead, every consumer fetches
// it from the upstream Apache Iceberg repo at the tag pinned in `spec-pin.json`.
// Override locally with the env vars below if you need to:
//
//   ICEBERG_SPEC_PATH   absolute path to a local YAML (skips the fetch)
//   ICEBERG_SPEC_URL    full URL to a YAML (forks, in-flight branches, etc.)

import { existsSync, readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const REPO_ROOT = resolve(__dirname, '..')

const pin = JSON.parse(readFileSync(resolve(REPO_ROOT, 'spec-pin.json'), 'utf-8'))

export const ICEBERG_REST_SPEC_TAG = pin.tag
export const ICEBERG_REST_SPEC_URL = `https://raw.githubusercontent.com/apache/iceberg/${pin.tag}/open-api/rest-catalog-open-api.yaml`

/**
 * Returns the OpenAPI YAML as a string. Memoized for the lifetime of the
 * process so repeated `loadSpecYaml()` calls don't refetch.
 */
let cached
export async function loadSpecYaml() {
  if (cached) return cached

  const envPath = process.env.ICEBERG_SPEC_PATH
  if (envPath) {
    if (!existsSync(envPath)) {
      throw new Error(`ICEBERG_SPEC_PATH points to a missing file: ${envPath}`)
    }
    cached = readFileSync(envPath, 'utf-8')
    return cached
  }

  const url = process.env.ICEBERG_SPEC_URL || ICEBERG_REST_SPEC_URL
  console.error(`[spec-source] fetching ${url}`)
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`Failed to fetch Iceberg spec from ${url}: ${res.status} ${res.statusText}`)
  }
  cached = await res.text()
  return cached
}
