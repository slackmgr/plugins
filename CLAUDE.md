# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

This is the `slackmgr/plugins` monorepo — a single repository hosting all Slack Manager plugins as independent Go modules. Each plugin lives in its own subdirectory and is versioned independently.

## Monorepo Structure

```
plugins/
├── .github/workflows/ci-sqs.yml      # CI for sqs (path-filtered)
├── .github/workflows/ci-dynamodb.yml # CI for dynamodb (path-filtered)
├── .github/workflows/ci-postgres.yml # CI for postgres (path-filtered)
├── .github/workflows/ci-pubsub.yml   # CI for pubsub (path-filtered)
├── .golangci.yaml             # Shared linter config (discovered by golangci-lint in each subdir)
├── sqs/                       # AWS SQS plugin — module: github.com/slackmgr/plugins/sqs
│   ├── go.mod
│   ├── go.sum
│   └── CLAUDE.md
├── dynamodb/                  # AWS DynamoDB plugin — module: github.com/slackmgr/plugins/dynamodb
│   ├── go.mod
│   ├── go.sum
│   └── CLAUDE.md
├── postgres/                  # PostgreSQL plugin — module: github.com/slackmgr/plugins/postgres
│   ├── go.mod
│   ├── go.sum
│   └── CLAUDE.md
├── pubsub/                    # GCP Pub/Sub plugin — module: github.com/slackmgr/plugins/pubsub
│   ├── go.mod
│   ├── go.sum
│   └── CLAUDE.md
└── <future-plugin>/           # Each new plugin follows the same pattern
```

There is **no root `go.mod`**. Each plugin is a fully independent Go module.

## Build Commands

Run all commands from within the relevant plugin subdirectory (e.g. `cd sqs`):

```bash
make test            # gosec, go fmt, go test (with -race and -cover), go vet
make lint            # golangci-lint (picks up root .golangci.yaml automatically)
make lint-fix        # golangci-lint --fix
make init            # go mod tidy
make bump-common-lib # update github.com/slackmgr/types to latest
```

**IMPORTANT:** Both `make test` and `make lint` MUST pass with zero errors before committing any changes. This applies regardless of whether the errors were introduced by your changes or existed previously.

## Keeping Documentation in Sync

After every code change, check whether the affected plugin's `README.md` needs updating. The README is the public-facing documentation and must always reflect the actual code.

## Tagging and Releases

### Tag convention

Tags are **prefixed with the plugin name** to support independent versioning of each module:

```
sqs/v0.2.0
dynamodb/v0.3.0
postgres/v0.3.3
pubsub/v0.1.0
<future-plugin>/v1.0.0
```

This follows the [Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source). The `go get` command resolves these automatically:

```bash
go get github.com/slackmgr/plugins/sqs@v0.2.0
```

### Release process

1. **Update the plugin's `CHANGELOG.md` first** — this is MANDATORY before creating any tag.
   - Review every commit since the last tag for this plugin: `git log <plugin>/v<last>..HEAD --oneline`
   - Every relevant commit MUST be represented under the correct section (`Added`, `Changed`, `Fixed`, `Removed`)
   - Add the new version section above `[Unreleased]` with today's date
   - Update the comparison links at the bottom of the file

2. **Commit the changelog:**
   ```bash
   git add <plugin>/CHANGELOG.md
   git commit -m "<plugin>: update CHANGELOG for v X.Y.Z"
   ```

3. **Create and push the tag:**
   ```bash
   git tag <plugin>/vX.Y.Z
   git push origin main
   git push origin <plugin>/vX.Y.Z
   ```

4. **Create the GitHub release:**
   ```bash
   gh release create <plugin>/vX.Y.Z --repo slackmgr/plugins --title "<plugin>/vX.Y.Z" --notes "..."
   ```
   Use the same content as the changelog entry for the release notes.

### Versioning

Follows [Semantic Versioning](https://semver.org/) per plugin:
- **Patch** (`Z`): bug fixes, CI/infra changes, documentation updates
- **Minor** (`Y`): new backwards-compatible features
- **Major** (`X`): breaking changes to the public API

### Rules

- **NEVER** create a tag without updating the plugin's `CHANGELOG.md` first
- **ALWAYS** review all commits since the last tag for that plugin — do not rely on memory or summaries
- Tags in this repo are plugin-scoped; a `sqs/vX.Y.Z` tag has no meaning for other plugins

## Adding a New Plugin

1. Create a subdirectory: `mkdir <plugin>`
2. Initialise a module: `cd <plugin> && go mod init github.com/slackmgr/plugins/<plugin>`
3. Add a `Makefile`, `README.md`, `CHANGELOG.md`, and `CLAUDE.md` following the `sqs/` pattern
4. Copy `.github/workflows/ci-sqs.yml` to `.github/workflows/ci-<plugin>.yml` and replace every occurrence of `sqs` with the new plugin name.
5. Add the plugin to the table in the root `README.md`
