# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `dynamodb/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-dynamodb-plugin](https://github.com/slackmgr/slack-manager-dynamodb-plugin) repository.

## [Unreleased]

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

[Unreleased]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.2...HEAD
[0.3.2]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.1...dynamodb/v0.3.2
[0.3.1]: https://github.com/slackmgr/plugins/compare/dynamodb/v0.3.0...dynamodb/v0.3.1
[0.3.0]: https://github.com/slackmgr/plugins/releases/tag/dynamodb/v0.3.0
