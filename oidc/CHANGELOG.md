# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `oidc/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

## [Unreleased]

### Added

- `WithPrincipalIDPrefix(string)`: prefix of principal IDs, default `oidc:` (`DefaultPrincipalIDPrefix`). A non-empty prefix must end with `:`, which the REST API does not allow in API key IDs, so OIDC principals cannot collide with API keys in access rules or client binding

### Changed

- **Breaking:** principal IDs have the prefix `oidc:` by default (e.g. `oidc:alertmanager`). Update access rules to the prefixed IDs (or match all with `oidc:*`), or use `WithPrincipalIDPrefix("")` to keep the old IDs
- **Breaking:** the maximum principal ID length, including the prefix, is `types.MaxClientLength` (100 characters, was 1000 bytes), so that the ID is never truncated when it is bound to the alert client. Tokens with longer IDs are rejected
- `WithPrincipalIDClaims` documents that only claims that clients and users cannot change may be used

## [0.1.0] - 2026-11-24

### Added

- `Authenticator`: a `types.Authenticator` for the Slack Manager REST API that validates OIDC/OAuth2 access tokens (JWTs) sent as bearer tokens: signature with the issuer's key set (asymmetric algorithms only), `iss`, `aud`, `exp`, `nbf` and `iat` with clock skew
- OIDC discovery, or a fixed key set URL (`WithJWKSURL`). HTTPS is required, except for localhost
- Configurable principal ID claims (default `client_id`, `azp`, `sub`), scope claims (default `scope`, `scp`), scope mapping and attribute claims
- Key set cache with rate limited downloads for tokens the cached keys cannot verify (`WithJWKSMinRefreshInterval`, default 1 minute) and a max age (`WithJWKSMaxAge`, default 1 hour)
- Non-JWT bearer tokens (e.g. API keys) return `types.ErrNoCredentials`, so the authenticator can be chained with the built-in API keys
- ID tokens with a `nonce` claim are rejected, and `WithRequiredTokenType` (e.g. `at+jwt`) can require a token type
- Stale key sets are refreshed in the background, downloads are detached from the request that triggered them, tokens of other issuers never trigger downloads, and keys are matched by key ID and algorithm
- The default HTTP client only follows redirects to https URLs (or http URLs of localhost)

[Unreleased]: https://github.com/slackmgr/plugins/compare/oidc/v0.1.0...HEAD
[0.1.0]: https://github.com/slackmgr/plugins/releases/tag/oidc/v0.1.0
