# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `postgres/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-postgres-plugin](https://github.com/slackmgr/slack-manager-postgres-plugin) repository.

## [Unreleased]

## [0.3.4] - 2026-02-22

### Changed

- Updated Go module dependencies
- CI: replaced shared matrix workflow with a dedicated per-plugin path-filtered workflow (`ci-postgres.yml`)
- CI: refactored Security job to install gosec via `go install` using the host Go toolchain, run from the module directory, use stable Go, and enforce failure on findings; gosec removed from golangci-lint
- CI: fixed `setup-go` `cache-dependency-path` for subdirectory modules
- Lint: updated golangci-lint config — disabled `godox`, enabled `sqlclosecheck`, disabled `testpackage`

## [0.3.3] - 2026-02-20

_Initial version in the slackmgr/plugins monorepo. For prior history see the
[slack-manager-postgres-plugin](https://github.com/slackmgr/slack-manager-postgres-plugin) repository._

### Changed

- Module path updated from `github.com/slackmgr/slack-manager-postgres-plugin` to `github.com/slackmgr/plugins/postgres`

[Unreleased]: https://github.com/slackmgr/plugins/compare/postgres/v0.3.4...HEAD
[0.3.4]: https://github.com/slackmgr/plugins/compare/postgres/v0.3.3...postgres/v0.3.4
[0.3.3]: https://github.com/slackmgr/plugins/releases/tag/postgres/v0.3.3
