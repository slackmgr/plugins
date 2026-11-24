// Package oidc authenticates Slack Manager REST API requests with OIDC/OAuth2 access tokens (JWTs).
//
// [Authenticator] implements [github.com/slackmgr/types.Authenticator]. It validates bearer tokens issued by an
// OpenID Connect identity provider (e.g. Keycloak, Okta, Auth0, Microsoft Entra ID, Google), typically obtained
// by monitoring systems and integrations with the OAuth2 client credentials flow, and maps their claims to a
// [github.com/slackmgr/types.Principal] with an ID and scopes.
//
//	authenticator, err := oidc.New(ctx, "https://login.example.com/realms/ops",
//	    oidc.WithAudience("slackmgr-api"),
//	)
//	if err != nil {
//	    return err
//	}
//
//	server := restapi.New(alertQueue, logger, apiCfg).
//	    WithAPIKeyAuth().                  // optional: API keys are tried first
//	    WithAuthenticator(authenticator)
//
// # Validation
//
// A token is accepted if it is signed with a key from the issuer's JSON Web Key Set using an allowed asymmetric
// algorithm (RS256, PS256 and ES256 by default; never HS256 or "none"), its iss claim is the issuer, its aud
// claim contains an accepted audience ([WithAudience], required), it has an exp claim, and exp, nbf and iat are
// valid within the clock skew ([WithClockSkew]). Tokens with a nonce claim (OIDC ID tokens) are rejected, and
// [WithRequiredTokenType] can require a token type such as "at+jwt".
//
// # Claims
//
// The principal ID is the first non-empty claim of [WithPrincipalIDClaims] (default client_id, azp, sub). The
// scopes are read from [WithScopeClaims] (default scope and scp), and can be renamed with [WithScopeMapping], e.g.
// for identity providers that cannot issue scopes named alerts:write.
//
// # Key set caching
//
// The key set is downloaded by [New], and cached. A token that the cached keys cannot verify (an unknown key ID,
// e.g. after a key rotation, or an invalid signature) triggers a new download, at most once per
// [WithJWKSMinRefreshInterval], so that forged tokens cannot make every request call the identity provider. The
// key set is also downloaded again (in the background) when it is older than [WithJWKSMaxAge], so that removed
// keys stop being accepted; if that download fails, the cached keys remain in use. Downloads are detached from
// the cancellation of the request that triggered them.
//
// # Errors
//
// [Authenticator.Authenticate] returns [github.com/slackmgr/types.ErrNoCredentials] for requests without a bearer
// token or with a bearer token that is not a JWT (such as an API key), so it can be chained with other
// authenticators. Invalid tokens return an error wrapping [github.com/slackmgr/types.ErrInvalidCredentials] (HTTP
// 401). If no key set has ever been downloaded, the error is an internal error (HTTP 500, see
// [IsKeySetUnavailable]).
package oidc
