import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import yaml from 'js-yaml'
import $RefParser from '@apidevtools/json-schema-ref-parser'
import Ajv, { type ValidateFunction } from 'ajv'
import addFormats from 'ajv-formats'

const SPEC_PATH = resolve(__dirname, '../../tmp/rest-catalog-open-api.yaml')

let cached: SpecBundle | undefined

interface OpenAPIDoc {
  paths: Record<string, Record<string, unknown>>
  components: { schemas: Record<string, unknown> }
}

export interface SpecBundle {
  doc: OpenAPIDoc
  ajv: Ajv
  /** Map of schema name → compiled validator. */
  validators: Map<string, ValidateFunction>
  /** Set of "{METHOD} {path-template}" strings — every operation defined in the spec. */
  operations: Set<string>
  /** Bare path-template (e.g. `/v1/{prefix}/namespaces`) → set of allowed methods. */
  pathMethods: Map<string, Set<string>>
}

/**
 * Load the bundled OpenAPI YAML once, dereference `$ref`s, register every named
 * schema in `components.schemas` with Ajv, and surface a small operation index
 * for path/method conformance checks.
 *
 * Ajv runs in non-strict mode because the spec uses OpenAPI-3.1-only keywords
 * (`discriminator`, examples on schemas, `nullable` in some places via 3.0
 * compat) that Ajv would otherwise reject.
 */
export async function loadSpec(): Promise<SpecBundle> {
  if (cached) return cached

  const raw = readFileSync(SPEC_PATH, 'utf-8')
  const parsed = yaml.load(raw) as OpenAPIDoc
  // `bundle` resolves external refs but preserves *internal* $refs, which Ajv
  // handles natively via `addSchema`. Using `dereference` here would inline
  // recursive schemas (Type/Schema/StructField, TableMetadata, etc.) and blow
  // the stack at compile time.
  const bundled = (await $RefParser.bundle(parsed as never)) as OpenAPIDoc

  const ajv = new Ajv({
    strict: false,
    allErrors: true,
    validateFormats: true,
    // OpenAPI uses a JSON-Schema-like dialect that doesn't 100% match Ajv's
    // defaults. Disabling unknown-keyword warnings keeps the output readable.
    logger: { log: () => {}, warn: () => {}, error: () => {} },
  })
  addFormats(ajv)

  // Register all schemas first by id so $refs across schemas resolve at
  // validation time. Use `#/components/schemas/<name>` as the canonical id.
  const cleanedSchemas: Record<string, Record<string, unknown>> = {}
  for (const [name, schema] of Object.entries(bundled.components.schemas)) {
    cleanedSchemas[name] = stripUnsupported(schema) as Record<string, unknown>
  }
  // Build a single root document that Ajv can traverse via $ref.
  const root = {
    $id: 'iceberg-rest-catalog',
    definitions: cleanedSchemas,
  }
  ajv.addSchema(root as object, root.$id)
  // Rewrite $refs from the spec form (`#/components/schemas/Foo`) to a form
  // that resolves inside our packaged root (`iceberg-rest-catalog#/definitions/Foo`).
  rewriteRefs(root)

  const validators = new Map<string, ValidateFunction>()
  const compileFailures: Record<string, string> = {}
  for (const name of Object.keys(cleanedSchemas)) {
    try {
      validators.set(name, ajv.compile({ $ref: `iceberg-rest-catalog#/definitions/${name}` }))
    } catch (err) {
      compileFailures[name] = err instanceof Error ? err.message : String(err)
    }
  }
  if (process.env.SPEC_COMPILE_DEBUG) {
    console.error(`[load-spec] ${Object.keys(compileFailures).length} schemas failed to compile:`)
    for (const [name, msg] of Object.entries(compileFailures)) {
      console.error(`  ${name}: ${msg}`)
    }
  }

  const operations = new Set<string>()
  const pathMethods = new Map<string, Set<string>>()
  for (const [path, ops] of Object.entries(bundled.paths)) {
    for (const method of Object.keys(ops)) {
      const m = method.toUpperCase()
      if (!HTTP_METHODS.has(m)) continue
      operations.add(`${m} ${path}`)
      let methods = pathMethods.get(path)
      if (!methods) {
        methods = new Set()
        pathMethods.set(path, methods)
      }
      methods.add(m)
    }
  }

  cached = { doc: bundled, ajv, validators, operations, pathMethods }
  return cached
}

const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])

/**
 * Recursively remove keywords Ajv can't process (`discriminator`, `example`,
 * etc.) so the schema compiles. We're validating on shape, not OpenAPI-3.1
 * polymorphism, which Ajv doesn't natively model.
 */
function stripUnsupported(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stripUnsupported)
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value)) {
      if (k === 'discriminator' || k === 'example' || k === 'externalDocs' || k === 'xml') continue
      out[k] = stripUnsupported(v)
    }
    return out
  }
  return value
}

/**
 * Rewrite `$ref` values in-place from the OpenAPI form
 * `#/components/schemas/Foo` to our packaged form
 * `iceberg-rest-catalog#/definitions/Foo`.
 */
function rewriteRefs(value: unknown): void {
  if (Array.isArray(value)) {
    for (const v of value) rewriteRefs(v)
    return
  }
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    if (typeof obj.$ref === 'string' && obj.$ref.startsWith('#/components/schemas/')) {
      const name = obj.$ref.slice('#/components/schemas/'.length)
      obj.$ref = `iceberg-rest-catalog#/definitions/${name}`
    }
    for (const v of Object.values(obj)) rewriteRefs(v)
  }
}

/**
 * Translate a concrete URL-like path into the spec's path template. For instance:
 *
 *   `/v1/namespaces/analytics/tables/events`
 *     → `/v1/{prefix}/namespaces/{namespace}/tables/{table}`
 *
 * Returns the matched template, or `undefined` if no spec path matches.
 *
 * The matching is best-effort: it handles simple `{var}` placeholders and the
 * server-prefix segment, which the client may emit either as `v1` (no warehouse)
 * or `v1/<warehouse>` (after /config). Both shapes are accepted.
 */
export function matchSpecPath(actualPath: string, specPaths: Iterable<string>): string | undefined {
  const actual = actualPath.startsWith('/') ? actualPath.slice(1) : actualPath
  const actualSegs = actual.split('/').filter(Boolean)

  for (const tpl of specPaths) {
    const tplNorm = tpl.startsWith('/') ? tpl.slice(1) : tpl
    const tplSegs = tplNorm.split('/').filter(Boolean)
    if (matchesSegments(tplSegs, actualSegs)) return tpl
  }
  return undefined
}

function matchesSegments(template: string[], actual: string[]): boolean {
  // The spec uses `{prefix}` as a single optional segment (warehouse id).
  // Real client requests may either emit it (after fetching /v1/config) or
  // skip it (no warehouse configured), so we try both interpretations.
  if (template.includes('{prefix}')) {
    return (
      compareTemplate(template, actual) ||
      compareTemplate(
        template.filter((seg) => seg !== '{prefix}'),
        actual
      )
    )
  }
  return compareTemplate(template, actual)
}

function compareTemplate(template: string[], actual: string[]): boolean {
  if (template.length !== actual.length) return false
  for (let i = 0; i < template.length; i++) {
    const t = template[i]
    if (t.startsWith('{') && t.endsWith('}')) continue
    if (t !== actual[i]) return false
  }
  return true
}
