# slackmgr/plugins

A monorepo for all [Slack Manager](https://github.com/slackmgr/core) plugins.

Each plugin is a separate Go module living in its own subdirectory. Modules are versioned independently using the [Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source): tags are prefixed with the plugin name (e.g. `sqs/v0.2.0`).

## Plugins

| Plugin | Module path | Description |
|--------|-------------|-------------|
| [sqs](./sqs/) | `github.com/slackmgr/plugins/sqs` | AWS SQS queue consumer and webhook handler |
| [dynamodb](./dynamodb/) | `github.com/slackmgr/plugins/dynamodb` | AWS DynamoDB storage backend |
| [postgres](./postgres/) | `github.com/slackmgr/plugins/postgres` | PostgreSQL storage backend |
| [pubsub](./pubsub/) | `github.com/slackmgr/plugins/pubsub` | Google Cloud Pub/Sub queue consumer and webhook handler |

## Versioning

Each plugin is versioned independently. To install a specific plugin:

```bash
go get github.com/slackmgr/plugins/sqs@v0.2.0
```

Or simply get the latest:

```bash
go get github.com/slackmgr/plugins/sqs
```

## License

MIT — see [LICENSE](./LICENSE) for details.
