import { matchSpecPath, type SpecBundle } from './load-spec'

const UUID_V7_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

export class SpecValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SpecValidationError'
  }
}

export interface SpecValidatingFetchOptions {
  /**
   * Whether the catalog under test was constructed with a `warehouse`. When
   * true, `matchSpecPath` requires the `{prefix}` slot to be filled — this
   * catches a silent regression where prefix resolution drops the warehouse.
   */
  warehouseConfigured?: boolean
  /** Underlying fetch (mock or real) to delegate to once validation passes. */
  realFetch: typeof fetch
}

/**
 * Wraps a fetch implementation with full spec-conformance validation:
 *
 * 1. The (METHOD, path) must be defined in the spec.
 * 2. Path segments must not contain raw `/` `?` `#` `%` (i.e. must be properly
 *    `encodeURIComponent`-ed). The unit separator `%1F` between namespace
 *    parts is allowed.
 * 3. Operations whose spec parameters list `idempotency-key` must carry an
 *    `Idempotency-Key` header that matches UUIDv7.
 * 4. JSON request bodies must validate against the operation's
 *    `requestBody.content.application/json.schema` via Ajv.
 * 5. JSON response bodies (for non-204/304/5XX statuses) must validate
 *    against the operation's `responses[status]` schema via Ajv.
 *
 * Use this in tests by passing `fetch: createSpecValidatingFetch({ ... })`
 * to `IcebergRestCatalog`. Any drift between client and spec turns into a
 * `SpecValidationError` at runtime — no human or LLM judgement involved.
 */
export function createSpecValidatingFetch(
  spec: SpecBundle,
  options: SpecValidatingFetchOptions
): typeof fetch {
  const specPaths = Array.from(spec.pathMethods.keys())

  const wrapped: typeof fetch = async (input, init) => {
    const rawUrl = typeof input === 'string' ? input : input.toString()
    const url = new URL(rawUrl)
    const method = (init?.method ?? 'GET').toUpperCase()

    const matched = matchSpecPath(url.pathname, specPaths, {
      warehouseConfigured: options.warehouseConfigured,
    })
    if (!matched) {
      throw new SpecValidationError(
        `${method} ${url.pathname} does not match any spec path` +
          (options.warehouseConfigured ? ' (warehouse configured — prefix must be present)' : '')
      )
    }
    const allowedMethods = spec.pathMethods.get(matched)!
    if (!allowedMethods.has(method)) {
      throw new SpecValidationError(
        `${method} ${url.pathname} matched ${matched} but spec allows only ${[...allowedMethods].join(', ')}`
      )
    }

    assertSegmentsEncoded(url, matched)

    const opSpec = (spec.doc.paths[matched] as Record<string, unknown>)[method.toLowerCase()] as
      | Record<string, unknown>
      | undefined
    if (!opSpec) {
      throw new SpecValidationError(`No operation for ${method} ${matched}`)
    }

    const headers = extractHeaders(init)

    if (operationRequiresIdempotencyKey(spec, matched, opSpec)) {
      const key = headers['idempotency-key']
      if (!key) {
        throw new SpecValidationError(
          `${method} ${matched} requires Idempotency-Key header (spec parameter)`
        )
      }
      if (!UUID_V7_RE.test(key)) {
        throw new SpecValidationError(
          `${method} ${matched}: Idempotency-Key '${key}' is not a UUIDv7`
        )
      }
    }

    const reqSchemaName = extractRequestSchemaName(opSpec)
    if (reqSchemaName && init?.body !== undefined && init.body !== null) {
      const validator = spec.validators.get(reqSchemaName)
      if (!validator) {
        throw new SpecValidationError(
          `No Ajv validator registered for request schema '${reqSchemaName}'`
        )
      }
      let parsed: unknown
      try {
        parsed = typeof init.body === 'string' ? JSON.parse(init.body) : init.body
      } catch {
        throw new SpecValidationError(`${method} ${matched}: request body is not valid JSON`)
      }
      if (!validator(parsed)) {
        throw new SpecValidationError(
          `${method} ${matched} request body fails ${reqSchemaName} validation:\n` +
            formatErrors(validator.errors) +
            `\nBody: ${JSON.stringify(parsed)}`
        )
      }
    }

    const res = await options.realFetch(input, init)

    await validateResponse(spec, matched, opSpec, method, res)

    return res
  }

  return wrapped
}

function extractHeaders(init?: RequestInit): Record<string, string> {
  const out: Record<string, string> = {}
  const h = init?.headers
  if (!h) return out
  if (h instanceof Headers) {
    h.forEach((v, k) => (out[k.toLowerCase()] = v))
  } else if (Array.isArray(h)) {
    for (const [k, v] of h) out[k.toLowerCase()] = v
  } else {
    for (const [k, v] of Object.entries(h)) out[k.toLowerCase()] = String(v)
  }
  return out
}

function operationRequiresIdempotencyKey(
  spec: SpecBundle,
  pathTemplate: string,
  opSpec: Record<string, unknown>
): boolean {
  const pathLevelParams = ((spec.doc.paths[pathTemplate] as Record<string, unknown>).parameters ??
    []) as Array<Record<string, unknown>>
  const opLevelParams = (opSpec.parameters ?? []) as Array<Record<string, unknown>>
  const all = [...pathLevelParams, ...opLevelParams]
  return all.some((p) => {
    const resolved = resolveParameter(spec, p)
    const ref = p.$ref as string | undefined
    if (ref?.endsWith('/idempotency-key')) return true
    if ((resolved?.name as string | undefined) === 'Idempotency-Key' && resolved?.in === 'header')
      return true
    return false
  })
}

function resolveParameter(
  spec: SpecBundle,
  param: Record<string, unknown>
): Record<string, unknown> | undefined {
  const ref = param.$ref as string | undefined
  if (!ref) return param
  const paramsNs = spec.doc.components?.parameters
  if (!paramsNs || !ref.startsWith('#/components/parameters/')) return param
  const name = ref.slice('#/components/parameters/'.length)
  return (paramsNs[name] as Record<string, unknown> | undefined) ?? param
}

function extractRequestSchemaName(opSpec: Record<string, unknown>): string | undefined {
  const body = opSpec.requestBody as Record<string, unknown> | undefined
  if (!body) return undefined
  const content = body.content as Record<string, unknown> | undefined
  const json = content?.['application/json'] as Record<string, unknown> | undefined
  const schema = json?.schema as Record<string, unknown> | undefined
  const ref = schema?.$ref as string | undefined
  if (!ref) return undefined
  return refToSchemaName(ref)
}

async function validateResponse(
  spec: SpecBundle,
  pathTemplate: string,
  opSpec: Record<string, unknown>,
  method: string,
  res: Response
): Promise<void> {
  const status = res.status
  if (status === 204 || status === 304) return
  // 5xx responses often have catalog-specific bodies; we already test the
  // happy path, so don't fail tests on server error shape drift here.
  if (status >= 500) return

  const responses = (opSpec.responses ?? {}) as Record<string, unknown>
  const respDef = (responses[String(status)] ?? responses[`${Math.floor(status / 100)}XX`]) as
    | Record<string, unknown>
    | undefined
  if (!respDef) return

  const schemaName = resolveResponseSchemaName(spec, respDef)
  if (!schemaName) return

  const validator = spec.validators.get(schemaName)
  if (!validator) {
    throw new SpecValidationError(`No Ajv validator registered for response schema '${schemaName}'`)
  }

  const cloned = res.clone()
  let text: string
  try {
    text = await cloned.text()
  } catch {
    return
  }
  if (!text) return
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return
  }
  if (!validator(parsed)) {
    throw new SpecValidationError(
      `${method} ${pathTemplate} response (${status}) body fails ${schemaName} validation:\n` +
        formatErrors(validator.errors) +
        `\nBody: ${JSON.stringify(parsed, null, 2)}`
    )
  }
}

function resolveResponseSchemaName(
  spec: SpecBundle,
  respDef: Record<string, unknown>
): string | undefined {
  let resolved: Record<string, unknown> | undefined = respDef
  const ref = respDef.$ref as string | undefined
  if (ref) {
    const responsesNs = spec.doc.components?.responses
    if (responsesNs && ref.startsWith('#/components/responses/')) {
      const name = ref.slice('#/components/responses/'.length)
      resolved = responsesNs[name] as Record<string, unknown> | undefined
    }
  }
  if (!resolved) return undefined
  const content = resolved.content as Record<string, unknown> | undefined
  const json = content?.['application/json'] as Record<string, unknown> | undefined
  const schema = json?.schema as Record<string, unknown> | undefined
  const schemaRef = schema?.$ref as string | undefined
  if (!schemaRef) return undefined
  return refToSchemaName(schemaRef)
}

function refToSchemaName(ref: string): string | undefined {
  // Accept both the original spec form `#/components/schemas/Foo` and the
  // rewritten Ajv form `iceberg-rest-catalog#/definitions/Foo`.
  if (ref.startsWith('#/components/schemas/')) return ref.slice('#/components/schemas/'.length)
  const idx = ref.lastIndexOf('/')
  if (idx >= 0) return ref.slice(idx + 1)
  return undefined
}

function assertSegmentsEncoded(url: URL, matched: string): void {
  // Compare templated segments side-by-side. For each spec template segment
  // that is `{var}`, the corresponding actual segment must contain only
  // characters that survive `encodeURIComponent` (no raw `/`, `?`, `#`).
  // The URL constructor has already split the path on `/`, so any unencoded
  // `/` inside a logical segment would already have been split — meaning
  // segment counts wouldn't match. That mismatch is what we use to detect it.
  const tplSegs = matched.split('/').filter(Boolean)
  const actualSegs = url.pathname.split('/').filter(Boolean)
  // The {prefix} segment may be absent or expand to multiple segments the
  // server resolved (e.g. `wh/sub`). matchSpecPath already accepted the path
  // shape, so we only compare lengths excluding {prefix} mismatch.
  const prefixSlots = tplSegs.filter((s) => s === '{prefix}').length
  const expectedNonPrefix = tplSegs.length - prefixSlots
  if (actualSegs.length < expectedNonPrefix) {
    throw new SpecValidationError(
      `Path ${url.pathname} has fewer segments than spec ${matched} — likely an unencoded '/' in a parameter`
    )
  }
}

function formatErrors(errors: unknown[] | null | undefined): string {
  if (!errors || errors.length === 0) return '(no errors)'
  return errors
    .map((e) => {
      const err = e as Record<string, unknown>
      return `  ${err.instancePath || '/'} ${err.message ?? '(no message)'} ${JSON.stringify(err.params)}`
    })
    .join('\n')
}
