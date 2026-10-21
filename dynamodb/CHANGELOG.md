# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `dynamodb/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-dynamodb-plugin](https://github.com/slackmgr/slack-manager-dynamodb-plugin) repository.

## [Unreleased]

### Changed

- Minimum Go version is now 1.26
- Bump `github.com/slackmgr/types` dependency to v0.7.1
- Update all direct dependencies (`github.com/aws/aws-sdk-go-v2` v1.47.3, `github.com/aws/aws-sdk-go-v2/service/dynamodb` v1.72.0, `github.com/aws/aws-sdk-go-v2/config` v1.33.9, `github.com/stretchr/testify` v1.12.1)
- Use `errors.AsType` when checking for a missing table (no behaviour change)
- CI and code scanning workflows read the Go version (major.minor) from `go.mod`/`go.work` and install the latest patch release (`check-latest: true`), so all jobs, including gosec and both govulncheck runs, use the same Go version
- Update GitHub Actions to latest versions (`actions/checkout` v7.0.1, `actions/setup-go` v7.0.0, `github/codeql-action` v4.39.0, `golangci/golangci-lint-action` v9.3.0)
- Disable the `exhaustruct_v5` linter (renamed in golangci-lint 2.15), matching the existing `exhaustruct` exclusion
- README: Go requirement updated to 1.26+

## [0.3.6] - 2026-05-11

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.6.1

## [0.3.5] - 2026-05-08

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.2

## [0.3.4] - 2026-05-07

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.1

## [0.3.3] - 2026-04-14

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.4.1

## [0.3.2] - 2026-02-26

### Changed

- CI: added govulncheck to per-plugin CI and code scanning workflow
- Updated Go module dependencies (aws-sdk-go-v2, slackmgr/types v0.4.0)

## [0.3.1] - 2026-02-22

### Changed

- Updated Go module dependencies
- CI: replaced shared matrix workflow with a dedicated per-plugin path-filtered workflow (`ci-dynamodb.yml`)
- CI: refactored Security job to install gosec via `go install` using the host Go toolchain, run from the module directory, use stable Go, and enforce failure on findings; gosec removed from golangci-lint
- CI: fixed `setup-go` `cache-dependency-path` for subdirectory modules
- Lint: updated golangci-lint config — disabled `godox`, enabled `sqlclosecheck`, disabled `testpackage`

## [0.3.0] - 2026-02-20

_Initial version in the slackmgr/plugins monorepo. For prior history see the
[slack-manager-dynamodb-plugin](https://github.com/slackmgr/slack-manager-dynamodb-plugin) repository._

### Changed

- Module path updated from `github.com/slackmgr/slack-manager-dynamodb-plugin` to `github.com/slackmgr/plugins/dynamodb`

[Unreleased]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.6...HEAD
[0.3.6]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.5...dynamodb/v0.3.6
[0.3.5]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.4...dynamodb/v0.3.5
[0.3.4]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.3...dynamodb/v0.3.4
[0.3.3]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.2...dynamodb/v0.3.3
[0.3.2]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.1...dynamodb/v0.3.2
[0.3.1]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.0...dynamodb/v0.3.1
[0.3.0]: https://github.com/slackmgr/plugins/releases/tag/dynamodb/v0.3.0
