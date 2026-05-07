import pin from '../spec-pin.json' with { type: 'json' }

/**
 * Git tag of `apache/iceberg` that this client is aligned to. The
 * spec-conformance test suite fetches and validates against the OpenAPI YAML
 * from this exact ref, so types, request bodies, and response handling here
 * match the spec at this version.
 *
 * @example 'apache-iceberg-1.10.0'
 */
export const ICEBERG_REST_SPEC_TAG: string = pin.tag

/**
 * Direct link to the OpenAPI YAML this client tracks.
 */
export const ICEBERG_REST_SPEC_URL: string = `https://raw.githubusercontent.com/apache/iceberg/${pin.tag}/open-api/rest-catalog-open-api.yaml`
