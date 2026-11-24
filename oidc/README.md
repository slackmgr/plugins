# slackmgr/plugins/oidc

OIDC/OAuth2 access token authentication for the [Slack Manager](https://github.com/slackmgr/core) REST API.

`oidc.Authenticator` implements `types.Authenticator`. It validates JWT access tokens issued by an OpenID Connect identity provider (Keycloak, Okta, Auth0, Microsoft Entra ID, Google, ...), typically obtained by monitoring systems and integrations with the OAuth2 client credentials flow, and maps their claims to a principal with an ID and scopes.

## Installation

```bash
go get github.com/slackmgr/plugins/oidc
```

## Usage

```go
authenticator, err := oidc.New(ctx, "https://login.example.com/realms/ops",
    oidc.WithAudience("slackmgr-api"),
    oidc.WithLogger(logger),
)
if err != nil {
    log.Fatal(err)
}

server := restapi.New(alertQueue, logger, apiCfg).
    WithSettings(apiSettings).
    WithAPIKeyAuth().                 // optional: API keys are tried first
    WithAuthenticator(authenticator)
```

`New` fetches the issuer's discovery document and key set, so it fails if the identity provider is unreachable or misconfigured. The issuer must match the `iss` claim of the tokens exactly. URLs must use HTTPS, except for `localhost`.

Clients send tokens as `Authorization: Bearer <token>`. With the Go client, use `client.WithAuthTokenSource` with an `oauth2` token source (see the [go-client README](https://github.com/slackmgr/go-client#authentication)). Prometheus Alertmanager supports the client credentials flow natively with `http_config.oauth2`.

## Validation

A token is accepted if:

- It is signed with a key from the issuer's JSON Web Key Set, using an allowed algorithm (RS256, PS256 and ES256 by default). Symmetric algorithms (HS256) and `none` are never accepted
- `iss` is the issuer, and `aud` contains one of the audiences of `WithAudience` (required)
- It has an `exp` claim, and `exp`, `nbf` and `iat` are valid within the clock skew (default 1 minute)
- It has a principal ID claim
- It has no `nonce` claim: OIDC ID tokens issued to a login flow must not be usable as access tokens. To reject ID tokens entirely, require the RFC 9068 access token type with `WithRequiredTokenType("at+jwt")`, if your identity provider sets it

Use an audience that identifies the Slack Manager API (an API identifier or resource), not the client ID of an application that users log in to: ID tokens have that client ID as audience.

Bearer tokens that are not JWTs (e.g. Slack Manager API keys) are left to other authenticators (`types.ErrNoCredentials`), so `oidc` can be combined with the built-in API keys.

## Claims

| Principal field | Source | Option |
|---|---|---|
| `ID` | `oidc:` + first non-empty claim of `client_id`, `azp`, `sub` | `WithPrincipalIDPrefix`, `WithPrincipalIDClaims` |
| `Scopes` | `scope` (space-separated string) and `scp` (string or array) | `WithScopeClaims`, `WithScopeMapping` |
| `Attributes` | `auth_method` (`oidc`), `issuer`, and selected string claims | `WithAttributeClaims` |

The Slack Manager scopes are `alerts:write`, `alerts:test`, `config:read` and `*`. If your identity provider cannot issue scopes with these names, map them:

```go
oidc.WithScopeClaims("roles"),
oidc.WithScopeMapping(map[string]string{
    "SlackManager.Alerts.Write": "alerts:write",
    "SlackManager.Admin":        "*",
}),
```

The principal ID is what the API logs, matches against channel access rules (`accessRules` in the API settings), and binds to the alert `client` (`APIConfig.Auth.ClientBinding`). It has the prefix `oidc:` (e.g. `oidc:alertmanager`), and API key IDs cannot contain `:`, so an OIDC client can never get the access rules of an API key with the same name. Match all OIDC clients in an access rule with `oidc:*`. With several `oidc` authenticators, give each its own prefix (e.g. `entra:`, `keycloak:`).

The principal ID, including the prefix, can be at most 100 characters (`types.MaxClientLength`): tokens with longer IDs are rejected (HTTP 401), never truncated.

Only use principal ID claims that the identity provider sets and that clients and users cannot change (`client_id`, `azp`, `sub`, `oid`). Never use editable claims such as `preferred_username`, `email` or `name`: a user could pick a value that matches another principal's access rules.

## Key set caching

The key set is cached. A token that the cached keys cannot verify (an unknown key ID after a key rotation, or an invalid signature) triggers a new download at most once per `WithJWKSMinRefreshInterval` (default 1 minute), so forged tokens cannot make every request call the identity provider. After a key rotation, tokens signed with the new key are accepted at the latest after this interval.

The key set is also downloaded again when it is older than `WithJWKSMaxAge` (default 1 hour), so that keys removed by the identity provider stop being accepted. This happens in the background: requests keep using the cached keys meanwhile, so a slow identity provider does not stall them, and if the download fails, the cached keys remain in use. Downloads are detached from the request that triggered them, so a cancelled request cannot block them. Only when no key set was ever downloaded is a token an internal error (HTTP 500).

Tokens from other issuers are rejected before the key set is consulted, so chaining several `oidc` authenticators (one per issuer) causes no extra downloads. A key is only used for a token if its key ID (if the token has one; keys without a key ID are the fallback) and algorithm (if the key has one) match.

## Options

| Option | Default | Description |
|---|---|---|
| `WithAudience(...string)` | — (required) | Accepted audiences; the token's `aud` must contain one |
| `WithRequiredTokenType(...string)` | any | Required `typ` header values, e.g. `at+jwt` |
| `WithSigningAlgorithms(...string)` | `RS256`, `PS256`, `ES256` | Allowed algorithms (RS\*, PS\*, ES\*, EdDSA only) |
| `WithPrincipalIDClaims(...string)` | `client_id`, `azp`, `sub` | Claims for the principal ID, in order of preference |
| `WithPrincipalIDPrefix(string)` | `oidc:` | Prefix of principal IDs; must end with `:` (empty disables it) |
| `WithScopeClaims(...string)` | `scope`, `scp` | Claims with scopes (space-separated string or array) |
| `WithScopeMapping(map[string]string)` | — | Rename token scopes to Slack Manager scopes |
| `WithAttributeClaims(...string)` | — | String claims copied to the principal's attributes |
| `WithClockSkew(time.Duration)` | `1m` | Tolerated clock difference (0–5m) |
| `WithHTTPClient(*http.Client)` | 10s timeout, https-only redirects | HTTP client for discovery and the key set. A custom client should not follow redirects to plain HTTP either |
| `WithJWKSURL(string)` | discovered | Key set URL, skips discovery |
| `WithJWKSMinRefreshInterval(time.Duration)` | `1m` | Minimum time between downloads for unverifiable tokens (1s–1h) |
| `WithJWKSMaxAge(time.Duration)` | `1h` | Maximum age of the cached key set (1m–7d) |
| `WithLogger(types.Logger)` | no-op | Logs key set downloads and failures |

## License

MIT — see [LICENSE](../LICENSE) for details.
