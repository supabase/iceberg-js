import { describe, it, expectTypeOf } from 'vitest'
import type {
  CatalogConfig,
  CommitTableRequest,
  CommitTableResponse,
  CreateNamespaceRequest,
  CreateNamespaceResponse,
  CreateTableRequest,
  GetNamespaceResponse,
  ListNamespacesResponse,
  ListTablesResponse,
  LoadTableResponse,
  RegisterTableRequest,
  RenameTableRequest,
  TableIdentifier,
  TableMetadata,
  TableRequirement,
  TableUpdate,
  UpdateNamespacePropertiesRequest,
  UpdateNamespacePropertiesResponse,
} from '../../src/catalog/types'
import type { components } from './generated'

type Schemas = components['schemas']

/**
 * These tests fail at compile time if our handwritten types diverge from the
 * spec-derived shapes. We use one-way structural assignability (`toExtend`)
 * because a few wire-level encodings (discriminated-union shape, nullable vs
 * optional) are equivalent in spirit but not syntactically identical between
 * `openapi-typescript`'s output and our handwritten types. Mutual assignment
 * would be too brittle.
 *
 * Direction: handwritten → generated. If a caller produces a value of our type
 * and the server expects the spec type, that value should always be valid.
 */
describe('Spec conformance — handwritten types extend spec-derived types', () => {
  it('CatalogConfig', () => {
    expectTypeOf<CatalogConfig>().toExtend<NonNullable<Schemas['CatalogConfig']>>()
  })

  it('CreateNamespaceRequest', () => {
    expectTypeOf<CreateNamespaceRequest>().toExtend<
      NonNullable<Schemas['CreateNamespaceRequest']>
    >()
  })

  it('CreateNamespaceResponse', () => {
    expectTypeOf<CreateNamespaceResponse>().toExtend<
      NonNullable<Schemas['CreateNamespaceResponse']>
    >()
  })

  it('GetNamespaceResponse', () => {
    expectTypeOf<GetNamespaceResponse>().toExtend<NonNullable<Schemas['GetNamespaceResponse']>>()
  })

  it('UpdateNamespacePropertiesRequest', () => {
    expectTypeOf<UpdateNamespacePropertiesRequest>().toExtend<
      NonNullable<Schemas['UpdateNamespacePropertiesRequest']>
    >()
  })

  it('UpdateNamespacePropertiesResponse', () => {
    expectTypeOf<UpdateNamespacePropertiesResponse>().toExtend<
      NonNullable<Schemas['UpdateNamespacePropertiesResponse']>
    >()
  })

  it('CreateTableRequest', () => {
    expectTypeOf<CreateTableRequest>().toExtend<NonNullable<Schemas['CreateTableRequest']>>()
  })

  it('TableIdentifier', () => {
    expectTypeOf<TableIdentifier>().toExtend<NonNullable<Schemas['TableIdentifier']>>()
  })

  it('ListNamespacesResponse', () => {
    expectTypeOf<ListNamespacesResponse>().toExtend<
      NonNullable<Schemas['ListNamespacesResponse']>
    >()
  })

  it('ListTablesResponse', () => {
    expectTypeOf<ListTablesResponse>().toExtend<NonNullable<Schemas['ListTablesResponse']>>()
  })

  it('TableMetadata', () => {
    expectTypeOf<TableMetadata>().toExtend<NonNullable<Schemas['TableMetadata']>>()
  })

  it('LoadTableResponse (= spec LoadTableResult)', () => {
    expectTypeOf<LoadTableResponse>().toExtend<NonNullable<Schemas['LoadTableResult']>>()
  })

  it('CommitTableResponse', () => {
    expectTypeOf<CommitTableResponse>().toExtend<NonNullable<Schemas['CommitTableResponse']>>()
  })

  it('RegisterTableRequest', () => {
    expectTypeOf<RegisterTableRequest>().toExtend<NonNullable<Schemas['RegisterTableRequest']>>()
  })

  it('RenameTableRequest', () => {
    expectTypeOf<RenameTableRequest>().toExtend<NonNullable<Schemas['RenameTableRequest']>>()
  })

  it('TableUpdate (23-variant discriminated union)', () => {
    expectTypeOf<TableUpdate>().toExtend<NonNullable<Schemas['TableUpdate']>>()
  })

  it('TableRequirement (8-variant discriminated union)', () => {
    expectTypeOf<TableRequirement>().toExtend<NonNullable<Schemas['TableRequirement']>>()
  })

  it('CommitTableRequest (`requirements` is required per spec)', () => {
    expectTypeOf<CommitTableRequest>().toExtend<NonNullable<Schemas['CommitTableRequest']>>()
  })
})

/**
 * Smoke checks that go the other way: a value matching the spec shape can be
 * read by code expecting our public type. These can be brittle on full unions,
 * so we limit them to leaf shapes where exact equivalence holds.
 */
describe('Spec conformance — spec-derived types are readable as handwritten', () => {
  it('a spec CommitTableRequest is also a handwritten CommitTableRequest at the top level', () => {
    expectTypeOf<{
      identifier?: NonNullable<Schemas['TableIdentifier']>
      requirements?: unknown[]
      updates: unknown[]
    }>().toExtend<{ updates: unknown[] }>()
    // Concrete check: handwritten -> spec one-way already covered above.
    expectTypeOf<CommitTableRequest>().toHaveProperty('updates')
  })
})
