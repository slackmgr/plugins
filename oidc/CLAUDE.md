# CLAUDE.md

This file provides guidance to Claude Code when working with the `oidc` plugin. See also the root `CLAUDE.md` for monorepo-wide conventions.

## Module Overview

`github.com/slackmgr/plugins/oidc` provides a `types.Authenticator` for the Slack Manager REST API (`restapi.Server.WithAuthenticator`) that validates OIDC/OAuth2 access tokens (JWTs).

## Architecture

- **`authenticator.go`** — `New()` (discovery, initial key set download) and `Authenticate()`: bearer JWT extraction, signature verification, registered claim validation (`jwt.Claims.ValidateWithLeeway`), principal ID, scopes and attributes from claims.
- **`keyset.go`** — Key set cache. Downloads are serialized and rate limited (`WithJWKSMinRefreshInterval`) for tokens the cached keys cannot verify, so forged tokens cannot amplify traffic to the identity provider. Stale key sets (`WithJWKSMaxAge`) are refreshed in the background (requests keep using cached keys); on failure the cached keys stay in use. Downloads use a context detached from the request (`context.WithoutCancel` + timeout): a cancelled request must never fail a download, as that would block downloads for the min refresh interval.
- **`fetch.go`** — Discovery document and JSON fetching (1 MB limit), URL validation (https, or http for loopback only).
- **`options.go`** — Functional options and validation.

## Security rules

- Only asymmetric algorithms are allowed; never HS* or `none` (HS* with a public key as secret is a classic JWT attack).
- `exp` is required. `aud` must contain a configured audience (`WithAudience` is mandatory). Tokens with `nonce` (ID tokens) are rejected.
- Tokens of other issuers are rejected (invalid, not "no credentials", which permissive mode would let through) before the key set is consulted.
- The default HTTP client only follows redirects that pass `validateURL`.
- Errors must keep the `types.ErrNoCredentials` / `types.ErrInvalidCredentials` / internal distinction: non-JWT bearer tokens are "no credentials" (chaining with API keys), invalid tokens are invalid, and only "no key set at all" is internal.
- go-oidc is deliberately not used: its `RemoteKeySet` re-downloads the key set for every unverifiable token (no rate limit), and its verifier formats key set errors with `%v`, which loses the internal/invalid distinction.

## Dependencies

- `github.com/go-jose/go-jose/v4` — JWS/JWT parsing and verification
- `github.com/slackmgr/types` — `Authenticator`, `Principal`, error values
