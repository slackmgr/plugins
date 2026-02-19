# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `sqs/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-sqs-plugin](https://github.com/slackmgr/slack-manager-sqs-plugin) repository.

## [Unreleased]

## [0.2.0] - 2026-02-19

_Initial version in the slackmgr/plugins monorepo. For prior history see the
[slack-manager-sqs-plugin](https://github.com/slackmgr/slack-manager-sqs-plugin) repository._

### Changed

- Renamed shared types dependency from `github.com/slackmgr/slack-manager-common` to `github.com/slackmgr/types` (v0.3.0); all public API references updated accordingly
- Module path updated from `github.com/slackmgr/slack-manager-sqs-plugin` to `github.com/slackmgr/plugins/sqs`

[Unreleased]: https://github.com/slackmgr/plugins/compare/sqs/v0.2.0...HEAD
[0.2.0]: https://github.com/slackmgr/plugins/releases/tag/sqs/v0.2.0
