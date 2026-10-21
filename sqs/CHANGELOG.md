# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `sqs/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-sqs-plugin](https://github.com/slackmgr/slack-manager-sqs-plugin) repository.

## [Unreleased]

### Changed

- Minimum Go version is now 1.26
- Bump `github.com/slackmgr/types` dependency to v0.7.1
- Update all direct dependencies (`github.com/aws/aws-sdk-go-v2` v1.47.3, `github.com/aws/aws-sdk-go-v2/service/sqs` v1.52.5)
- Request the message group ID via `ReceiveMessageInput.MessageSystemAttributeNames` instead of the deprecated `AttributeNames` (no behaviour change)
- CI and code scanning workflows read the Go version (major.minor) from `go.mod`/`go.work` and install the latest patch release (`check-latest: true`), so all jobs, including gosec and both govulncheck runs, use the same Go version
- Update GitHub Actions to latest versions (`actions/checkout` v7.0.1, `actions/setup-go` v7.0.0, `github/codeql-action` v4.39.0, `golangci/golangci-lint-action` v9.3.0)
- Disable the `exhaustruct_v5` linter (renamed in golangci-lint 2.15), matching the existing `exhaustruct` exclusion
- README: Go requirement updated to 1.26+

## [0.2.7] - 2026-05-11

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.6.1

## [0.2.6] - 2026-05-08

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.2

## [0.2.5] - 2026-05-07

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.1

## [0.2.4] - 2026-04-14

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.4.1

## [0.2.3] - 2026-03-20

### Added

- `WithDisableMessageExtension()` option to skip the background visibility-timeout extension goroutine, for workloads with short, predictable processing times

## [0.2.2] - 2026-02-26

### Changed

- CI: added govulncheck to per-plugin CI and code scanning workflow
- Updated Go module dependencies (aws-sdk-go-v2, slackmgr/types v0.4.0)

## [0.2.1] - 2026-02-22

### Changed

- Updated Go module dependencies
- CI: replaced shared matrix workflow with a dedicated per-plugin path-filtered workflow (`ci-sqs.yml`)
- CI: refactored Security job to install gosec via `go install` using the host Go toolchain, run from the module directory, use stable Go, and enforce failure on findings; gosec removed from golangci-lint
- CI: fixed `setup-go` `cache-dependency-path` for subdirectory modules
- Lint: updated golangci-lint config — disabled `godox`, enabled `sqlclosecheck`, disabled `testpackage`

## [0.2.0] - 2026-02-19

_Initial version in the slackmgr/plugins monorepo. For prior history see the
[slack-manager-sqs-plugin](https://github.com/slackmgr/slack-manager-sqs-plugin) repository._

### Changed

- Renamed shared types dependency from `github.com/slackmgr/slack-manager-common` to `github.com/slackmgr/types` (v0.3.0); all public API references updated accordingly
- Module path updated from `github.com/slackmgr/slack-manager-sqs-plugin` to `github.com/slackmgr/plugins/sqs`

[Unreleased]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.7...HEAD
[0.2.7]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.6...sqs/v0.2.7
[0.2.6]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.5...sqs/v0.2.6
[0.2.5]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.4...sqs/v0.2.5
[0.2.4]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.3...sqs/v0.2.4
[0.2.3]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.2...sqs/v0.2.3
[0.2.2]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.1...sqs/v0.2.2
[0.2.1]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.0...sqs/v0.2.1
[0.2.0]: https://github.com/slackmgr/plugins/releases/tag/sqs/v0.2.0
