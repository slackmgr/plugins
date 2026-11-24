package oidc

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/slackmgr/types"
)

const (
	defaultClockSkew      = time.Minute
	maxClockSkew          = 5 * time.Minute
	defaultHTTPTimeout    = 10 * time.Second
	defaultJWKSMinRefresh = time.Minute
	minJWKSMinRefresh     = time.Second
	maxJWKSMinRefresh     = time.Hour
	defaultJWKSMaxAge     = time.Hour
	minJWKSMaxAge         = time.Minute
	maxJWKSMaxAge         = 7 * 24 * time.Hour
	maxClaimNameLength    = 200
)

// supportedAlgorithms are the asymmetric signature algorithms that can be allowed with [WithSigningAlgorithms].
// Symmetric algorithms (HS256 and others) and "none" are never accepted.
var supportedAlgorithms = []jose.SignatureAlgorithm{ //nolint:gochecknoglobals // Constant list
	jose.RS256, jose.RS384, jose.RS512,
	jose.PS256, jose.PS384, jose.PS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.EdDSA,
}

// Option is a functional option for configuring an [Authenticator].
type Option func(*options)

type options struct {
	audiences         []string
	requiredTypes     []string
	algorithms        []jose.SignatureAlgorithm
	principalIDClaims []string
	scopeClaims       []string
	scopeMapping      map[string]string
	attributeClaims   []string
	clockSkew         time.Duration
	httpClient        *http.Client
	jwksURL           string
	jwksMinRefresh    time.Duration
	jwksMaxAge        time.Duration
	logger            types.Logger
	now               func() time.Time
}

func newOptions() *options {
	return &options{
		algorithms:        []jose.SignatureAlgorithm{jose.RS256, jose.PS256, jose.ES256},
		principalIDClaims: []string{"client_id", "azp", "sub"},
		scopeClaims:       []string{"scope", "scp"},
		clockSkew:         defaultClockSkew,
		httpClient:        &http.Client{Timeout: defaultHTTPTimeout, CheckRedirect: checkRedirect},
		jwksMinRefresh:    defaultJWKSMinRefresh,
		jwksMaxAge:        defaultJWKSMaxAge,
		logger:            &types.NoopLogger{},
		now:               time.Now,
	}
}

// WithAudience sets the accepted audiences: a token must have at least one of them in its aud claim. Required:
// use the audience (API identifier or resource) that the identity provider issues access tokens for the Slack
// Manager API with. Empty values are ignored. Calling it again adds audiences.
//
// Do not use the client ID of an application that users log in to: OIDC ID tokens have that client ID as
// audience. ID tokens with a nonce claim are always rejected, and [WithRequiredTokenType] can reject the others.
func WithAudience(audiences ...string) Option {
	return func(o *options) {
		for _, a := range audiences {
			if a = strings.TrimSpace(a); a != "" {
				o.audiences = append(o.audiences, a)
			}
		}
	}
}

// WithRequiredTokenType requires the typ header of tokens to be one of the given types, e.g. "at+jwt" for access
// tokens in the format of RFC 9068, which rejects OIDC ID tokens. Types are compared case-insensitively, with or
// without the "application/" prefix. The default accepts any type, since many identity providers issue access
// tokens with typ "JWT".
func WithRequiredTokenType(types ...string) Option {
	return func(o *options) {
		o.requiredTypes = trimAll(types)
	}
}

// WithSigningAlgorithms sets the accepted signature algorithms, e.g. "RS256" or "ES256". The default is RS256,
// PS256 and ES256. Only asymmetric algorithms are supported: RS256/384/512, PS256/384/512, ES256/384/512 and
// EdDSA. Other values (including the symmetric HS256 and "none") are rejected by [New].
func WithSigningAlgorithms(algorithms ...string) Option {
	return func(o *options) {
		o.algorithms = make([]jose.SignatureAlgorithm, 0, len(algorithms))

		for _, a := range algorithms {
			o.algorithms = append(o.algorithms, jose.SignatureAlgorithm(strings.TrimSpace(a)))
		}
	}
}

// WithPrincipalIDClaims sets the claims that hold the principal ID ([types.Principal.ID]), in order of
// preference: the first claim with a non-empty string value is used. The default is "client_id", "azp", "sub",
// which identifies the client application for tokens from the OAuth2 client credentials flow with most identity
// providers. A token without any of them is rejected.
func WithPrincipalIDClaims(claims ...string) Option {
	return func(o *options) {
		o.principalIDClaims = trimAll(claims)
	}
}

// WithScopeClaims sets the claims that hold the scopes of the token. A claim can be a space-separated string
// (like "scope") or an array of strings (like "scp" or "roles"). The scopes of all claims are combined. The
// default is "scope" and "scp".
func WithScopeClaims(claims ...string) Option {
	return func(o *options) {
		o.scopeClaims = trimAll(claims)
	}
}

// WithScopeMapping maps token scopes to Slack Manager scopes (see [types.ScopeAlertsWrite] and the other
// scopes), for identity providers that cannot issue scopes with these names, e.g.
// {"api://slackmgr/Alerts.Write": "alerts:write"}. Scopes without a mapping are kept as they are. Calling it
// again adds mappings.
func WithScopeMapping(mapping map[string]string) Option {
	return func(o *options) {
		if o.scopeMapping == nil {
			o.scopeMapping = make(map[string]string, len(mapping))
		}

		for k, v := range mapping {
			o.scopeMapping[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
}

// WithAttributeClaims sets claims that are copied to [types.Principal.Attributes], e.g. "sub" or "tenant". Only
// string claims are copied. The attributes always include [AttributeAuthMethod] ("oidc") and [AttributeIssuer],
// which cannot be used as attribute claims.
func WithAttributeClaims(claims ...string) Option {
	return func(o *options) {
		o.attributeClaims = trimAll(claims)
	}
}

// WithClockSkew sets the tolerated clock difference with the identity provider, for the exp, nbf and iat
// claims. The default is 1 minute. Valid range is 0–5 minutes, checked by [New].
func WithClockSkew(skew time.Duration) Option {
	return func(o *options) {
		o.clockSkew = skew
	}
}

// WithHTTPClient sets the HTTP client for the discovery document and the key set. The default has a 10 second
// timeout, and only follows redirects to https URLs (or http URLs of localhost). A custom client should not follow
// redirects to other URLs either. Nil values are ignored.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) {
		if client != nil {
			o.httpClient = client
		}
	}
}

// WithJWKSURL sets the URL of the JSON Web Key Set, which skips OIDC discovery. Use it for issuers without a
// discovery document. By default, the URL is the jwks_uri of the discovery document.
func WithJWKSURL(url string) Option {
	return func(o *options) {
		o.jwksURL = strings.TrimSpace(url)
	}
}

// WithJWKSMinRefreshInterval sets the minimum time between key set downloads triggered by tokens that the cached
// keys cannot verify (an unknown key ID, e.g. after a key rotation, or an invalid signature). It protects the
// identity provider (and the API) from callers sending forged tokens. The default is 1 minute. Valid range is
// 1 second–1 hour, checked by [New].
func WithJWKSMinRefreshInterval(interval time.Duration) Option {
	return func(o *options) {
		o.jwksMinRefresh = interval
	}
}

// WithJWKSMaxAge sets how long the cached key set is used before it is downloaded again, so that keys removed by
// the identity provider (e.g. after a compromise) stop being accepted. If the download fails, the cached keys
// remain in use. The default is 1 hour. Valid range is 1 minute–7 days, and at least the minimum refresh
// interval, checked by [New].
func WithJWKSMaxAge(maxAge time.Duration) Option {
	return func(o *options) {
		o.jwksMaxAge = maxAge
	}
}

// WithLogger sets the logger for key set downloads and their failures. The default discards all logs. Nil values
// are ignored. Token validation failures are not logged here: the REST API logs failed authentications.
func WithLogger(logger types.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// withClock sets the time source, for tests.
func withClock(now func() time.Time) Option {
	return func(o *options) {
		o.now = now
	}
}

func (o *options) validate() error {
	if len(o.audiences) == 0 {
		return errors.New("at least one audience is required (WithAudience)")
	}

	if len(o.algorithms) == 0 {
		return errors.New("at least one signing algorithm is required (WithSigningAlgorithms)")
	}

	for _, a := range o.algorithms {
		if !slices.Contains(supportedAlgorithms, a) {
			return fmt.Errorf("signing algorithm %q is not supported: use asymmetric algorithms (RS*, PS*, ES*, EdDSA)", a)
		}
	}

	if err := validateClaimNames("principal ID claims (WithPrincipalIDClaims)", o.principalIDClaims, true); err != nil {
		return err
	}

	if err := validateClaimNames("scope claims (WithScopeClaims)", o.scopeClaims, false); err != nil {
		return err
	}

	if err := validateClaimNames("attribute claims (WithAttributeClaims)", o.attributeClaims, false); err != nil {
		return err
	}

	if slices.Contains(o.attributeClaims, AttributeAuthMethod) || slices.Contains(o.attributeClaims, AttributeIssuer) {
		return fmt.Errorf("attribute claims (WithAttributeClaims) cannot be %q or %q, which are set by the authenticator", AttributeAuthMethod, AttributeIssuer)
	}

	if slices.Contains(o.requiredTypes, "") {
		return errors.New("required token types (WithRequiredTokenType) cannot be empty")
	}

	for k, v := range o.scopeMapping {
		if k == "" || v == "" {
			return errors.New("scope mapping (WithScopeMapping) cannot have empty scopes")
		}
	}

	if o.clockSkew < 0 || o.clockSkew > maxClockSkew {
		return fmt.Errorf("clock skew (WithClockSkew) must be between 0 and %v", maxClockSkew)
	}

	if o.jwksMinRefresh < minJWKSMinRefresh || o.jwksMinRefresh > maxJWKSMinRefresh {
		return fmt.Errorf("JWKS min refresh interval (WithJWKSMinRefreshInterval) must be between %v and %v", minJWKSMinRefresh, maxJWKSMinRefresh)
	}

	if o.jwksMaxAge < minJWKSMaxAge || o.jwksMaxAge > maxJWKSMaxAge {
		return fmt.Errorf("JWKS max age (WithJWKSMaxAge) must be between %v and %v", minJWKSMaxAge, maxJWKSMaxAge)
	}

	if o.jwksMaxAge < o.jwksMinRefresh {
		return errors.New("JWKS max age (WithJWKSMaxAge) must be at least the min refresh interval (WithJWKSMinRefreshInterval)")
	}

	return nil
}

func validateClaimNames(what string, claims []string, required bool) error {
	if required && len(claims) == 0 {
		return fmt.Errorf("%s cannot be empty", what)
	}

	for _, c := range claims {
		if c == "" || len(c) > maxClaimNameLength {
			return fmt.Errorf("%s must be non-empty and at most %d characters", what, maxClaimNameLength)
		}
	}

	return nil
}

func trimAll(values []string) []string {
	result := make([]string, 0, len(values))

	for _, v := range values {
		result = append(result, strings.TrimSpace(v))
	}

	return result
}
