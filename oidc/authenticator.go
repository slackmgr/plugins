package oidc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/slackmgr/types"
)

// Attributes set on every principal (see [types.Principal.Attributes]).
const (
	// AttributeAuthMethod is the attribute with the authentication method, always [AuthMethod].
	AttributeAuthMethod = "auth_method"

	// AttributeIssuer is the attribute with the issuer of the token.
	AttributeIssuer = "issuer"

	// AuthMethod is the value of the [AttributeAuthMethod] attribute.
	AuthMethod = "oidc"
)

const (
	maxPrincipalIDLength = 1000
	maxScopes            = 100
	maxScopeLength       = 200
	maxAttributeLength   = 1000
)

// Authenticator authenticates requests with OIDC/OAuth2 access tokens (JWTs), sent as bearer tokens. It
// implements [types.Authenticator], for use with the Slack Manager REST API (restapi.Server.WithAuthenticator).
//
// A token is accepted if it is signed with a key from the issuer's key set, with an allowed algorithm, and its
// iss, aud, exp and nbf claims are valid. The principal ID and scopes are read from configurable claims.
//
// Bearer tokens that are not JWTs (e.g. Slack Manager API keys) are left to other authenticators: Authenticate
// returns [types.ErrNoCredentials] for them, so the authenticator can be chained with API keys (see
// [types.ChainAuthenticators]).
//
// Create it with [New]. It is safe for concurrent use.
type Authenticator struct {
	issuer  string
	options *options
	keys    *keySet
}

// New creates an [Authenticator] for tokens issued by issuerURL, e.g. "https://login.example.com/realms/ops".
// The issuer must match the iss claim of the tokens exactly.
//
// New fetches the OIDC discovery document of the issuer (unless [WithJWKSURL] is used) and the key set, so it
// fails if the identity provider is unreachable or misconfigured. URLs must use https, except for localhost.
// [WithAudience] is required.
func New(ctx context.Context, issuerURL string, opts ...Option) (*Authenticator, error) {
	o := newOptions()

	for _, opt := range opts {
		opt(o)
	}

	issuerURL = strings.TrimSpace(issuerURL)

	if err := validateURL("issuer URL", issuerURL); err != nil {
		return nil, err
	}

	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	jwksURL := o.jwksURL

	if jwksURL == "" {
		var err error

		if jwksURL, err = discoverJWKSURL(ctx, o.httpClient, issuerURL); err != nil {
			return nil, fmt.Errorf("OIDC discovery failed for issuer %s: %w", issuerURL, err)
		}
	}

	if err := validateURL("JWKS URL", jwksURL); err != nil {
		return nil, err
	}

	a := &Authenticator{
		issuer:  issuerURL,
		options: o,
		keys:    newKeySet(jwksURL, o),
	}

	if _, err := a.keys.candidates(ctx, "", "", false); err != nil {
		return nil, fmt.Errorf("failed to download the key set from %s: %w", jwksURL, err)
	}

	return a, nil
}

// Issuer returns the issuer URL of the authenticator.
func (a *Authenticator) Issuer() string {
	return a.issuer
}

// Authenticate implements [types.Authenticator]. It returns [types.ErrNoCredentials] if the request has no bearer
// token, or one that is not a JWT, and an error wrapping [types.ErrInvalidCredentials] if the token is invalid.
// Other errors (the key set cannot be downloaded) are internal errors.
func (a *Authenticator) Authenticate(r *http.Request) (*types.Principal, error) {
	token, ok := bearerJWT(r)
	if !ok {
		return nil, types.ErrNoCredentials
	}

	claims, err := a.verify(r.Context(), token)
	if err != nil {
		return nil, err
	}

	return a.principal(claims)
}

// bearerJWT returns the bearer token of the request, if it looks like a signed JWT (three dot-separated parts).
func bearerJWT(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}

	token = strings.TrimSpace(token)

	if strings.Count(token, ".") != 2 {
		return "", false
	}

	return token, true
}

// verify checks the signature and the registered claims of the token, and returns all its claims.
func (a *Authenticator) verify(ctx context.Context, token string) (map[string]any, error) {
	parsed, err := jwt.ParseSigned(token, a.options.algorithms)
	if err != nil {
		return nil, fmt.Errorf("malformed token or disallowed algorithm: %w", types.ErrInvalidCredentials)
	}

	header := parsed.Headers[0] // compact serialization: exactly one signature

	if err := a.checkType(header); err != nil {
		return nil, err
	}

	// Reject tokens of other issuers before looking up keys, so that they do not trigger key set downloads (e.g.
	// when several authenticators for different issuers are chained). The issuer is verified again below.
	var unverified jwt.Claims

	if err := parsed.UnsafeClaimsWithoutVerification(&unverified); err != nil || unverified.Issuer != a.issuer {
		return nil, fmt.Errorf("token is not issued by %s: %w", a.issuer, types.ErrInvalidCredentials)
	}

	var (
		registered jwt.Claims
		claims     map[string]any
	)

	verified := false

	// Try the cached keys first, then download the key set once (rate limited) if none of them verifies the
	// token, e.g. after a key rotation.
	for _, refresh := range []bool{false, true} {
		keys, err := a.keys.candidates(ctx, header.KeyID, header.Algorithm, refresh)
		if err != nil {
			return nil, err
		}

		if verified = verifyWithKeys(parsed, keys, &registered, &claims); verified {
			break
		}
	}

	if !verified {
		return nil, fmt.Errorf("invalid signature or unknown key: %w", types.ErrInvalidCredentials)
	}

	if registered.Expiry == nil {
		return nil, fmt.Errorf("token has no exp claim: %w", types.ErrInvalidCredentials)
	}

	expected := jwt.Expected{
		Issuer:      a.issuer,
		AnyAudience: jwt.Audience(a.options.audiences),
		Time:        a.options.now(),
	}

	if err := registered.ValidateWithLeeway(expected, a.options.clockSkew); err != nil {
		return nil, fmt.Errorf("%w: %w", err, types.ErrInvalidCredentials)
	}

	// OIDC ID tokens (which have a nonce when issued to a login flow) must not be used as access tokens.
	if _, ok := claims["nonce"]; ok {
		return nil, fmt.Errorf("token has a nonce claim (an ID token, not an access token): %w", types.ErrInvalidCredentials)
	}

	return claims, nil
}

// checkType checks the typ header against the required token types, if any (see [WithRequiredTokenType]). Types
// are compared case-insensitively, with or without the "application/" prefix (RFC 7515).
func (a *Authenticator) checkType(header jose.Header) error {
	if len(a.options.requiredTypes) == 0 {
		return nil
	}

	typ, _ := header.ExtraHeaders[jose.HeaderType].(string)
	typ = normalizeType(typ)

	for _, required := range a.options.requiredTypes {
		if typ == normalizeType(required) {
			return nil
		}
	}

	return fmt.Errorf("token type %q is not accepted: %w", typ, types.ErrInvalidCredentials)
}

func normalizeType(typ string) string {
	typ = strings.ToLower(strings.TrimSpace(typ))
	return strings.TrimPrefix(typ, "application/")
}

// verifyWithKeys verifies the signature of the token with each key until one succeeds, and decodes its claims.
func verifyWithKeys(parsed *jwt.JSONWebToken, keys []jose.JSONWebKey, registered *jwt.Claims, claims *map[string]any) bool {
	for _, key := range keys {
		if err := parsed.Claims(key.Key, registered, claims); err == nil {
			return true
		}
	}

	return false
}

// principal builds the principal from the claims of a verified token.
func (a *Authenticator) principal(claims map[string]any) (*types.Principal, error) {
	id := ""

	for _, claim := range a.options.principalIDClaims {
		if s, ok := claims[claim].(string); ok && strings.TrimSpace(s) != "" {
			id = strings.TrimSpace(s)
			break
		}
	}

	if id == "" {
		return nil, fmt.Errorf("token has none of the principal ID claims %v: %w", a.options.principalIDClaims, types.ErrInvalidCredentials)
	}

	if len(id) > maxPrincipalIDLength {
		return nil, fmt.Errorf("principal ID is longer than %d characters: %w", maxPrincipalIDLength, types.ErrInvalidCredentials)
	}

	attributes := map[string]string{
		AttributeAuthMethod: AuthMethod,
		AttributeIssuer:     a.issuer,
	}

	for _, claim := range a.options.attributeClaims {
		if s, ok := claims[claim].(string); ok && s != "" && len(s) <= maxAttributeLength {
			attributes[claim] = s
		}
	}

	return &types.Principal{
		ID:         id,
		Scopes:     a.scopes(claims),
		Attributes: attributes,
	}, nil
}

// scopes returns the scopes of the scope claims, mapped with the scope mapping, without duplicates.
func (a *Authenticator) scopes(claims map[string]any) []string {
	set := make(map[string]struct{})

	add := func(scope string) {
		scope = strings.TrimSpace(scope)
		if scope == "" || len(scope) > maxScopeLength || len(set) >= maxScopes {
			return
		}

		if mapped, ok := a.options.scopeMapping[scope]; ok {
			scope = mapped
		}

		set[scope] = struct{}{}
	}

	for _, claim := range a.options.scopeClaims {
		switch v := claims[claim].(type) {
		case string:
			for scope := range strings.FieldsSeq(v) {
				add(scope)
			}
		case []any:
			for _, item := range v {
				if scope, ok := item.(string); ok {
					add(scope)
				}
			}
		}
	}

	return slices.Sorted(maps.Keys(set))
}

// IsKeySetUnavailable reports whether err (returned by [Authenticator.Authenticate]) means that the key set of the
// identity provider could not be downloaded.
func IsKeySetUnavailable(err error) bool {
	return errors.Is(err, errKeysUnavailable)
}
