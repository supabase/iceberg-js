# Contributing to `iceberg-js`

Thank you for your interest in contributing to `iceberg-js`! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Getting Started

`iceberg-js` is a TypeScript library that provides a client for the Apache Iceberg REST Catalog API. Before contributing, please:

1. Read the [README.md](./README.md) to understand the project's scope and goals
2. Read our [Code of Conduct](https://github.com/supabase/.github/blob/main/CODE_OF_CONDUCT.md)
3. Check existing [issues](https://github.com/supabase/iceberg-js/issues) and [pull requests](https://github.com/supabase/iceberg-js/pulls) to avoid duplicate work

## Development Setup

### Prerequisites

- **Node.js**: 20.x or higher
- **pnpm**

### Installation

1. Fork and clone the repository:

```bash
git clone https://github.com/YOUR_USERNAME/iceberg-js.git
cd iceberg-js
```

2. Install dependencies:

```bash
pnpm install
```

3. Build the project to verify setup:

```bash
pnpm run build
```

## Project Structure

```text
iceberg-js/
├── src/                          # Source code
│   ├── catalog/                  # Catalog operations
│   │   ├── IcebergRestCatalog.ts # Main catalog client
│   │   ├── namespaces.ts         # Namespace operations
│   │   ├── tables.ts             # Table operations
│   │   └── types.ts              # TypeScript type definitions
│   ├── http/                     # HTTP client layer
│   │   ├── HttpClient.ts         # HTTP client interface
│   │   ├── createFetchClient.ts  # Fetch-based implementation
│   │   └── types.ts              # HTTP-related types
│   ├── errors/                   # Error handling
│   │   └── IcebergError.ts       # Custom error class
│   ├── utils/                    # Utility functions
│   └── index.ts                  # Public API exports
├── test/                         # Tests
│   ├── catalog/                  # Unit tests for catalog operations
│   ├── http/                     # Unit tests for HTTP client
│   └── integration/              # Integration tests
├── scripts/                      # Build and test scripts
├── dist/                         # Compiled output (generated)
├── .github/workflows/            # CI/CD configuration
├── tsconfig.json                 # TypeScript configuration
├── tsup.config.ts                # Build configuration
├── vitest.config.ts              # Test configuration
├── eslint.config.ts              # Linting configuration
└── .prettierrc                   # Code formatting configuration
```

## Development Workflow

### Building

Build the library for distribution:

```bash
pnpm run build
```

Watch mode for development (rebuilds on file changes):

```bash
pnpm run dev
```

### Type Checking

Run TypeScript type checking without emitting files:

```bash
pnpm run type-check
```

### Formatting

Format all code using Prettier:

```bash
pnpm run format
```

### Linting

Run ESLint to check for code issues:

```bash
pnpm run lint
```

### Full Check

Run all checks (lint, type-check, test, build):

```bash
pnpm run check
```

This is what CI runs, so it's a good idea to run this before pushing.

## Testing

### Unit Tests

Run unit tests with Vitest:

```bash
pnpm test
```

Watch mode for test-driven development:

```bash
pnpm test:watch
```

### Integration Tests

Integration tests run against a real Iceberg REST Catalog in Docker:

```bash
pnpm test:integration
```

This command:

1. Starts Docker Compose services (Iceberg REST catalog + MinIO storage)
2. Runs integration tests
3. Leaves services running for debugging

For CI or cleanup after testing:

```bash
pnpm test:integration:ci
```

This adds automatic cleanup (stops and removes containers).

### Writing Tests

- Unit tests go in `test/` with the same structure as `src/`
- Use descriptive test names that explain the behavior being tested
- Mock external dependencies (HTTP calls) in unit tests
- Integration tests should test real catalog interactions
- Follow existing test patterns for consistency

## Code Style

### TypeScript Guidelines

- Use **strict TypeScript** - all code must pass type checking
- Prefer **interfaces** for public APIs, **types** for unions/intersections
- Export types that consumers might need
- Use **explicit return types** on public methods
- Avoid `any` - use `unknown` if type is truly unknown

### Naming Conventions

- **Classes**: PascalCase (e.g., `IcebergRestCatalog`)
- **Interfaces/Types**: PascalCase (e.g., `TableIdentifier`)
- **Functions/Methods**: camelCase (e.g., `createTable`)
- **Constants**: UPPER_SNAKE_CASE for true constants (e.g., `DEFAULT_TIMEOUT`)
- **Private members**: prefix with underscore or use private keyword

### Documentation

- All public APIs must have **TSDoc comments**
- Include `@param`, `@returns`, `@throws`, and `@example` tags where appropriate
- Examples in TSDoc should be runnable code
- Keep documentation concise but informative

Example:

````typescript
/**
 * Creates a new table in the catalog.
 *
 * @param namespace - Namespace to create the table in
 * @param request - Table creation request including name, schema, partition spec
 * @returns Table metadata for the created table
 * @throws {IcebergError} If the table already exists or namespace doesn't exist
 *
 * @example
 * ```typescript
 * const metadata = await catalog.createTable(
 *   { namespace: ['analytics'] },
 *   { name: 'events', schema: { ... } }
 * );
 * ```
 */
async createTable(namespace: NamespaceIdentifier, request: CreateTableRequest): Promise<TableMetadata>
````

### Error Handling

- Use `IcebergError` for all catalog-related errors
- Include status code and error type from the REST API
- Provide helpful error messages
- Let unexpected errors propagate (don't catch everything)

## Submitting Changes

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) for automated releases. Format:

```text
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**

- `feat`: New feature (triggers minor version bump)
- `fix`: Bug fix (triggers patch version bump)
- `docs`: Documentation changes only
- `test`: Adding or updating tests
- `chore`: Maintenance tasks, dependency updates
- `refactor`: Code changes that neither fix bugs nor add features
- `perf`: Performance improvements
- `ci`: CI/CD configuration changes

**Breaking changes:**

- Use `feat!:` or `fix!:` for breaking changes (triggers major version bump)
- Or include `BREAKING CHANGE:` in the commit footer

**Examples:**

```bash
feat: add support for view operations
fix: handle empty namespace list correctly
feat(auth): add OAuth2 authentication support
docs: update README with new examples
test: add integration tests for table updates
feat!: change auth config structure

BREAKING CHANGE: auth configuration now uses a discriminated union
```

### Pull Request Process

1. **Create a branch** from `main`:

   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make your changes** following the guidelines above

3. **Run checks** locally:

   ```bash
   pnpm run check
   pnpm test:integration
   ```

4. **Commit** using conventional commit format:

   ```bash
   git commit -m "feat: add support for XYZ"
   ```

5. **Push** to your fork:

   ```bash
   git push origin feat/my-feature
   ```

6. **Open a Pull Request** with:
   - Clear title following conventional commit format
   - Description of what changed and why
   - Reference any related issues (e.g., "Fixes #123")
   - Screenshots/examples if adding user-facing features

7. **Respond to feedback** - maintainers may request changes

8. **Wait for CI** - all tests must pass before merging

### PR Guidelines

- Keep PRs focused - one feature or fix per PR
- Update documentation if you change public APIs
- Add tests for new functionality
- Ensure all CI checks pass
- Rebase on `main` if needed to resolve conflicts
- Be responsive to review feedback

## Release Process

This project uses [release-please](https://github.com/googleapis/release-please) for automated releases. You don't need to manually manage versions or changelogs.

### How It Works

1. **You commit** using conventional commit format (see above)

2. **release-please creates/updates a release PR** automatically when changes are pushed to `main`
   - Updates version in `package.json`
   - Updates `CHANGELOG.md`
   - Generates release notes

3. **Maintainer merges the release PR** when ready to release
   - Creates a GitHub release and git tag
   - Automatically publishes to npm with provenance

### Version Bumps

Versions follow [Semantic Versioning](https://semver.org/):

- **Major (1.0.0 → 2.0.0)**: Breaking changes (`feat!:`, `fix!:`, or `BREAKING CHANGE:`)
- **Minor (1.0.0 → 1.1.0)**: New features (`feat:`)
- **Patch (1.0.0 → 1.0.1)**: Bug fixes (`fix:`)

Commits with types like `docs:`, `test:`, `chore:` don't trigger releases on their own.

### For Maintainers Only

Publishing is fully automated via GitHub Actions:

1. Merge the release-please PR when ready
2. GitHub Actions will automatically publish to npm with provenance
3. No manual `npm publish` needed

## Questions?

- Open an [issue](https://github.com/supabase/iceberg-js/issues) for bugs or feature requests
- Check existing issues and PRs before creating new ones
- Tag your issues appropriately (`bug`, `enhancement`, `documentation`, etc.)

## License

By contributing to `iceberg-js`, you agree that your contributions will be licensed under the MIT License.
