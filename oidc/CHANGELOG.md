# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `oidc/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

## [Unreleased]

### Added

- `Authenticator`: a `types.Authenticator` for the Slack Manager REST API that validates OIDC/OAuth2 access tokens (JWTs) sent as bearer tokens: signature with the issuer's key set (asymmetric algorithms only), `iss`, `aud`, `exp`, `nbf` and `iat` with clock skew
- OIDC discovery, or a fixed key set URL (`WithJWKSURL`). HTTPS is required, except for localhost
- Configurable principal ID claims (default `client_id`, `azp`, `sub`), scope claims (default `scope`, `scp`), scope mapping and attribute claims
- Key set cache with rate limited downloads for tokens the cached keys cannot verify (`WithJWKSMinRefreshInterval`, default 1 minute) and a max age (`WithJWKSMaxAge`, default 1 hour)
- Non-JWT bearer tokens (e.g. API keys) return `types.ErrNoCredentials`, so the authenticator can be chained with the built-in API keys
- ID tokens with a `nonce` claim are rejected, and `WithRequiredTokenType` (e.g. `at+jwt`) can require a token type
- Stale key sets are refreshed in the background, downloads are detached from the request that triggered them, tokens of other issuers never trigger downloads, and keys are matched by key ID and algorithm
- The default HTTP client only follows redirects to https URLs (or http URLs of localhost)

[Unreleased]: https://github.com/slackmgr/plugins/commits/main/oidc
