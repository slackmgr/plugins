package oidc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/slackmgr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAudience = "slackmgr-api"

// testKeys are generated once, as RSA key generation is slow.
var testKeys = sync.OnceValue(func() map[string]any {
	rsa1, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	rsa2, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	return map[string]any{"rsa1": rsa1, "rsa2": rsa2, "ec": ec}
})

// fakeClock is a settable clock.
type fakeClock struct {
	now atomic.Pointer[time.Time]
}

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.set(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))

	return c
}

func (c *fakeClock) Now() time.Time          { return *c.now.Load() }
func (c *fakeClock) advance(d time.Duration) { c.set(c.Now().Add(d)) }
func (c *fakeClock) set(t time.Time)         { c.now.Store(&t) }

// fakeIDP is an identity provider serving a discovery document and a key set over TLS.
type fakeIDP struct {
	server *httptest.Server

	mu            sync.Mutex
	published     []string // key names in the key set; the key ID is the name
	issuer        string   // issuer in the discovery document, defaults to the server URL
	failJWKS      bool
	jwksBody      string // overrides the key set response
	discoveryCode int
	jwksGate      chan struct{} // if set, key set requests wait until it is closed
	redirectJWKS  string        // if set, key set requests are redirected to this URL
	stripKeyIDs   bool          // publish the keys without key IDs

	jwksFetches atomic.Int32
}

func newFakeIDP(t *testing.T, published ...string) *fakeIDP {
	t.Helper()

	idp := &fakeIDP{published: published, discoveryCode: http.StatusOK}

	idp.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idp.mu.Lock()
		gate := idp.jwksGate
		idp.mu.Unlock()

		if gate != nil && r.URL.Path == "/jwks" {
			<-gate
		}

		idp.mu.Lock()
		defer idp.mu.Unlock()

		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			if idp.discoveryCode != http.StatusOK {
				w.WriteHeader(idp.discoveryCode)
				return
			}

			issuer := idp.issuer
			if issuer == "" {
				issuer = idp.server.URL
			}

			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "jwks_uri": idp.server.URL + "/jwks"})
		case "/jwks":
			idp.jwksFetches.Add(1)

			if idp.redirectJWKS != "" {
				http.Redirect(w, r, idp.redirectJWKS, http.StatusFound)
				return
			}

			if idp.failJWKS {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if idp.jwksBody != "" {
				_, _ = w.Write([]byte(idp.jwksBody))
				return
			}

			set := jose.JSONWebKeySet{}

			for _, name := range idp.published {
				key := publicJWK(name)
				if idp.stripKeyIDs {
					key.KeyID = ""
				}

				set.Keys = append(set.Keys, key)
			}

			_ = json.NewEncoder(w).Encode(set)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	t.Cleanup(idp.server.Close)

	return idp
}

func (idp *fakeIDP) set(f func(idp *fakeIDP)) {
	idp.mu.Lock()
	defer idp.mu.Unlock()

	f(idp)
}

func publicJWK(name string) jose.JSONWebKey {
	switch k := testKeys()[name].(type) {
	case *rsa.PrivateKey:
		return jose.JSONWebKey{Key: &k.PublicKey, KeyID: name, Algorithm: string(jose.RS256), Use: "sig"}
	case *ecdsa.PrivateKey:
		return jose.JSONWebKey{Key: &k.PublicKey, KeyID: name, Algorithm: string(jose.ES256), Use: "sig"}
	default:
		panic("unknown key " + name)
	}
}

// sign returns a JWT with the claims, signed with the named key (and its name as key ID).
func sign(t *testing.T, keyName string, claims map[string]any) string {
	t.Helper()

	alg := jose.RS256
	if keyName == "ec" {
		alg = jose.ES256
	}

	return signWith(t, keyName, alg, "JWT", claims)
}

// signWith returns a JWT with the claims, signed with the named key and algorithm, with the given typ header.
func signWith(t *testing.T, keyName string, alg jose.SignatureAlgorithm, typ string, claims map[string]any) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: testKeys()[keyName]},
		(&jose.SignerOptions{}).WithType(jose.ContentType(typ)).WithHeader(jose.HeaderKey("kid"), keyName))
	require.NoError(t, err)

	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	return token
}

func (idp *fakeIDP) claims(clock *fakeClock, extra map[string]any) map[string]any {
	claims := map[string]any{
		"iss":       idp.server.URL,
		"aud":       testAudience,
		"exp":       clock.Now().Add(time.Hour).Unix(),
		"iat":       clock.Now().Unix(),
		"client_id": "alertmanager",
		"scope":     "alerts:write openid",
	}

	for k, v := range extra {
		if v == nil {
			delete(claims, k)
		} else {
			claims[k] = v
		}
	}

	return claims
}

func newTestAuthenticator(t *testing.T, idp *fakeIDP, clock *fakeClock, opts ...Option) *Authenticator {
	t.Helper()

	opts = append([]Option{WithAudience(testAudience), WithHTTPClient(idp.server.Client()), withClock(clock.Now)}, opts...)

	a, err := New(t.Context(), idp.server.URL, opts...)
	require.NoError(t, err)

	return a
}

func authenticate(t *testing.T, a *Authenticator, authorization string) (*types.Principal, error) {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/alerts", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}

	return a.Authenticate(req)
}

func TestAuthenticator_Authenticate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		claims  map[string]any // changes to the default claims, nil values remove claims
		opts    []Option
		header  func(token string) string
		advance time.Duration
		wantID  string
		wantErr error
	}{
		{name: "valid token", key: "rsa1", wantID: "oidc:alertmanager"},
		{name: "valid EC token", key: "ec", wantID: "oidc:alertmanager"},
		{name: "ID token with nonce", key: "rsa1", claims: map[string]any{"nonce": "n-1"}, wantErr: types.ErrInvalidCredentials},
		{name: "lower-case scheme", key: "rsa1", header: func(tok string) string { return "bearer " + tok }, wantID: "oidc:alertmanager"},
		{name: "audience in array", key: "rsa1", claims: map[string]any{"aud": []string{"other", testAudience}}, wantID: "oidc:alertmanager"},
		{name: "one of several accepted audiences", key: "rsa1", claims: map[string]any{"aud": "second"}, opts: []Option{WithAudience("second")}, wantID: "oidc:alertmanager"},
		{name: "wrong audience", key: "rsa1", claims: map[string]any{"aud": "other"}, wantErr: types.ErrInvalidCredentials},
		{name: "no audience", key: "rsa1", claims: map[string]any{"aud": nil}, wantErr: types.ErrInvalidCredentials},
		{name: "wrong issuer", key: "rsa1", claims: map[string]any{"iss": "https://evil.example.com"}, wantErr: types.ErrInvalidCredentials},
		{name: "expired", key: "rsa1", advance: 2 * time.Hour, wantErr: types.ErrInvalidCredentials},
		{name: "expired within clock skew", key: "rsa1", advance: time.Hour + 30*time.Second, wantID: "oidc:alertmanager"},
		{name: "expired beyond clock skew", key: "rsa1", advance: time.Hour + 30*time.Second, opts: []Option{WithClockSkew(0)}, wantErr: types.ErrInvalidCredentials},
		{name: "no exp", key: "rsa1", claims: map[string]any{"exp": nil}, wantErr: types.ErrInvalidCredentials},
		{name: "not yet valid", key: "rsa1", claims: map[string]any{"nbf": time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC).Unix()}, wantErr: types.ErrInvalidCredentials},
		{name: "issued in the future", key: "rsa1", claims: map[string]any{"iat": time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC).Unix()}, wantErr: types.ErrInvalidCredentials},
		{name: "unpublished key", key: "rsa2", wantErr: types.ErrInvalidCredentials},
		{name: "algorithm not allowed", key: "ec", opts: []Option{WithSigningAlgorithms("RS256")}, wantErr: types.ErrInvalidCredentials},
		{name: "azp when no client_id", key: "rsa1", claims: map[string]any{"client_id": nil, "azp": "grafana"}, wantID: "oidc:grafana"},
		{name: "sub as last resort", key: "rsa1", claims: map[string]any{"client_id": nil, "sub": "svc-1"}, wantID: "oidc:svc-1"},
		{name: "empty client_id is skipped", key: "rsa1", claims: map[string]any{"client_id": " ", "sub": "svc-1"}, wantID: "oidc:svc-1"},
		{name: "custom principal ID claim", key: "rsa1", claims: map[string]any{"appid": "app-1"}, opts: []Option{WithPrincipalIDClaims("appid")}, wantID: "oidc:app-1"},
		{name: "no principal ID", key: "rsa1", claims: map[string]any{"client_id": nil}, wantErr: types.ErrInvalidCredentials},
		{name: "non-string principal ID", key: "rsa1", claims: map[string]any{"client_id": 42}, wantErr: types.ErrInvalidCredentials},
		{name: "too long principal ID", key: "rsa1", claims: map[string]any{"client_id": strings.Repeat("a", types.MaxClientLength-len(DefaultPrincipalIDPrefix)+1)}, wantErr: types.ErrInvalidCredentials},
		{name: "max length principal ID with prefix", key: "rsa1", claims: map[string]any{"client_id": strings.Repeat("é", types.MaxClientLength-len(DefaultPrincipalIDPrefix))}, wantID: DefaultPrincipalIDPrefix + strings.Repeat("é", types.MaxClientLength-len(DefaultPrincipalIDPrefix))},
		{name: "custom prefix", key: "rsa1", opts: []Option{WithPrincipalIDPrefix("entra:")}, wantID: "entra:alertmanager"},
		{name: "no prefix", key: "rsa1", opts: []Option{WithPrincipalIDPrefix("")}, wantID: "alertmanager"},
		{name: "max length principal ID without prefix", key: "rsa1", claims: map[string]any{"client_id": strings.Repeat("a", types.MaxClientLength)}, opts: []Option{WithPrincipalIDPrefix("")}, wantID: strings.Repeat("a", types.MaxClientLength)},
		{name: "no header", wantErr: types.ErrNoCredentials},
		{name: "basic auth", header: func(string) string { return "Basic dXNlcjpwYXNz" }, wantErr: types.ErrNoCredentials},
		{name: "API key bearer token", header: func(string) string { return "Bearer 3f2a9c0b5d" }, wantErr: types.ErrNoCredentials},
		{name: "malformed JWT", header: func(string) string { return "Bearer a.b.c" }, wantErr: types.ErrInvalidCredentials},
		{
			name: "tampered payload", key: "rsa1",
			header: func(tok string) string {
				parts := strings.Split(tok, ".")
				parts[1] = parts[1][:len(parts[1])-2] + "xx"

				return "Bearer " + strings.Join(parts, ".")
			},
			wantErr: types.ErrInvalidCredentials,
		},
		{
			name: "unsigned token", key: "rsa1",
			header: func(tok string) string {
				parts := strings.Split(tok, ".")
				return "Bearer eyJhbGciOiJub25lIn0." + parts[1] + "."
			},
			wantErr: types.ErrInvalidCredentials,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock()
			idp := newFakeIDP(t, "rsa1", "ec")
			a := newTestAuthenticator(t, idp, clock, tt.opts...)

			token := ""
			if tt.key != "" {
				token = sign(t, tt.key, idp.claims(clock, tt.claims))
			}

			header := "Bearer " + token
			if tt.header != nil {
				header = tt.header(token)
			} else if tt.key == "" {
				header = ""
			}

			clock.advance(tt.advance)

			p, err := authenticate(t, a, header)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, p)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantID, p.ID)
		})
	}
}

func TestAuthenticator_TokenTypeAndKeySelection(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	idp := newFakeIDP(t, "rsa1", "ec")

	t.Run("required token type", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock, WithRequiredTokenType("at+jwt"))

		for typ, wantOK := range map[string]bool{"at+jwt": true, "AT+JWT": true, "application/at+jwt": true, "JWT": false, "": false} {
			_, err := authenticate(t, a, "Bearer "+signWith(t, "rsa1", jose.RS256, typ, idp.claims(clock, nil)))
			if wantOK {
				require.NoError(t, err, typ)
			} else {
				require.ErrorIs(t, err, types.ErrInvalidCredentials, typ)
			}
		}
	})

	t.Run("any token type by default", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock)

		_, err := authenticate(t, a, "Bearer "+signWith(t, "rsa1", jose.RS256, "at+jwt", idp.claims(clock, nil)))
		require.NoError(t, err)
	})

	t.Run("key algorithm must match the token", func(t *testing.T) {
		t.Parallel()

		// rsa1 is published with alg RS256, so a PS256 signature with the same key is rejected.
		a := newTestAuthenticator(t, idp, clock)

		_, err := authenticate(t, a, "Bearer "+signWith(t, "rsa1", jose.PS256, "JWT", idp.claims(clock, nil)))
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
	})

	t.Run("keys without key ID", func(t *testing.T) {
		t.Parallel()

		other := newFakeIDP(t, "rsa1")
		other.set(func(idp *fakeIDP) { idp.stripKeyIDs = true })
		a := newTestAuthenticator(t, other, clock)

		p, err := authenticate(t, a, "Bearer "+sign(t, "rsa1", other.claims(clock, nil)))
		require.NoError(t, err)
		assert.Equal(t, "oidc:alertmanager", p.ID)
	})

	t.Run("other issuers do not trigger downloads", func(t *testing.T) {
		t.Parallel()

		own := newFakeIDP(t, "rsa1")
		other := newFakeIDP(t, "rsa2")
		clock := newFakeClock()
		a := newTestAuthenticator(t, own, clock)

		foreign := sign(t, "rsa2", other.claims(clock, nil))
		clock.advance(2 * time.Minute)

		for range 3 {
			_, err := authenticate(t, a, "Bearer "+foreign)
			require.ErrorIs(t, err, types.ErrInvalidCredentials)
		}

		assert.EqualValues(t, 1, own.jwksFetches.Load())
	})
}

func TestCheckRedirect(t *testing.T) {
	t.Parallel()

	t.Run("redirect to http is not followed", func(t *testing.T) {
		t.Parallel()

		target := newFakeIDP(t, "rsa1")
		idp := newFakeIDP(t, "rsa1")
		idp.set(func(idp *fakeIDP) { idp.redirectJWKS = "http://keys.example.com/jwks" })

		client := idp.server.Client()
		client.CheckRedirect = checkRedirect

		_, err := New(t.Context(), idp.server.URL, WithAudience(testAudience), WithHTTPClient(client))
		require.Error(t, err)
		assert.Contains(t, err.Error(), `redirect URL "http://keys.example.com/jwks" must use https`)

		// A redirect to https is followed.
		idp.set(func(idp *fakeIDP) { idp.redirectJWKS = target.server.URL + "/jwks" })

		_, err = New(t.Context(), idp.server.URL, WithAudience(testAudience), WithHTTPClient(client))
		require.NoError(t, err)
	})

	t.Run("too many redirects", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
		require.NoError(t, checkRedirect(req, make([]*http.Request, maxRedirects-1)))
		require.ErrorContains(t, checkRedirect(req, make([]*http.Request, maxRedirects)), "stopped after 5 redirects")
	})

	t.Run("default client uses it", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, newOptions().httpClient.CheckRedirect)
	})
}

func TestAuthenticator_PrincipalScopesAndAttributes(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	idp := newFakeIDP(t, "rsa1")

	t.Run("default scope claims", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock)
		token := sign(t, "rsa1", idp.claims(clock, map[string]any{
			"scope": "alerts:write  openid alerts:write",
			"scp":   []any{"config:read", 42, ""},
			"roles": []any{"admin"},
		}))

		p, err := authenticate(t, a, "Bearer "+token)
		require.NoError(t, err)
		assert.Equal(t, []string{"alerts:write", "config:read", "openid"}, p.Scopes)
		assert.True(t, p.HasScope(types.ScopeAlertsWrite))
		assert.False(t, p.HasScope(types.ScopeAlertsTest))
		assert.Equal(t, map[string]string{"auth_method": "oidc", "issuer": idp.server.URL}, p.Attributes)
	})

	t.Run("custom scope claims, mapping and attributes", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock,
			WithScopeClaims("roles"),
			WithScopeMapping(map[string]string{"Alerts.Write": types.ScopeAlertsWrite}),
			WithScopeMapping(map[string]string{"Admin": types.ScopeAll}),
			WithAttributeClaims("sub", "tid", "groups"),
		)

		token := sign(t, "rsa1", idp.claims(clock, map[string]any{
			"roles":  []any{"Alerts.Write", "Other", "Admin"},
			"sub":    "object-1",
			"tid":    "tenant-1",
			"groups": []any{"not", "a", "string"},
		}))

		p, err := authenticate(t, a, "Bearer "+token)
		require.NoError(t, err)
		assert.Equal(t, []string{"*", "Other", "alerts:write"}, p.Scopes)
		assert.Equal(t, map[string]string{"auth_method": "oidc", "issuer": idp.server.URL, "sub": "object-1", "tid": "tenant-1"}, p.Attributes)
	})

	t.Run("no scopes", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock)
		token := sign(t, "rsa1", idp.claims(clock, map[string]any{"scope": nil}))

		p, err := authenticate(t, a, "Bearer "+token)
		require.NoError(t, err)
		assert.Empty(t, p.Scopes)
	})

	t.Run("scopes are capped", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, clock)

		many := make([]string, 0, 150)
		for i := range 150 {
			many = append(many, "s"+strings.Repeat("x", i%50)+string(rune('a'+i%26))+strings.Repeat("y", i/26))
		}

		token := sign(t, "rsa1", idp.claims(clock, map[string]any{"scope": strings.Join(many, " ") + " " + strings.Repeat("z", 201)}))

		p, err := authenticate(t, a, "Bearer "+token)
		require.NoError(t, err)
		assert.Len(t, p.Scopes, 100)
	})
}

func TestAuthenticator_KeyRotationAndRateLimit(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	idp := newFakeIDP(t, "rsa1")
	a := newTestAuthenticator(t, idp, clock)
	require.EqualValues(t, 1, idp.jwksFetches.Load(), "New downloads the key set")

	valid := sign(t, "rsa1", idp.claims(clock, nil))
	rotated := sign(t, "rsa2", idp.claims(clock, nil))

	// Cached keys verify without downloads.
	for range 3 {
		_, err := authenticate(t, a, "Bearer "+valid)
		require.NoError(t, err)
	}

	assert.EqualValues(t, 1, idp.jwksFetches.Load())

	// An unknown key ID triggers a download, but not within the min refresh interval.
	clock.advance(2 * time.Minute)

	for range 5 {
		_, err := authenticate(t, a, "Bearer "+rotated)
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
	}

	assert.EqualValues(t, 2, idp.jwksFetches.Load(), "one download per min refresh interval")

	// After a key rotation, the new key is found by the next allowed download.
	idp.set(func(idp *fakeIDP) { idp.published = []string{"rsa1", "rsa2"} })

	_, err := authenticate(t, a, "Bearer "+rotated)
	require.ErrorIs(t, err, types.ErrInvalidCredentials, "still rate limited")

	clock.advance(time.Minute)

	p, err := authenticate(t, a, "Bearer "+rotated)
	require.NoError(t, err)
	assert.Equal(t, "oidc:alertmanager", p.ID)
	assert.EqualValues(t, 3, idp.jwksFetches.Load())

	// A forged signature with a known key ID also counts against the rate limit.
	clock.advance(30 * time.Second)

	forged := strings.Split(valid, ".")
	forged[2] = strings.Split(rotated, ".")[2]

	for range 3 {
		_, err := authenticate(t, a, "Bearer "+strings.Join(forged, "."))
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
	}

	assert.EqualValues(t, 3, idp.jwksFetches.Load())
}

func TestAuthenticator_MaxAgeAndOutages(t *testing.T) {
	t.Parallel()

	t.Run("stale keys are refreshed, and removed keys stop working", func(t *testing.T) {
		t.Parallel()

		clock := newFakeClock()
		idp := newFakeIDP(t, "rsa1", "rsa2")
		a := newTestAuthenticator(t, idp, clock, WithJWKSMaxAge(10*time.Minute))

		token := func() string { return sign(t, "rsa2", idp.claims(clock, nil)) }

		_, err := authenticate(t, a, "Bearer "+token())
		require.NoError(t, err)

		idp.set(func(idp *fakeIDP) { idp.published = []string{"rsa1"} })

		_, err = authenticate(t, a, "Bearer "+token())
		require.NoError(t, err, "cached until the max age")

		clock.advance(10 * time.Minute)

		// The stale key set is used while it is refreshed in the background.
		_, err = authenticate(t, a, "Bearer "+token())
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			snap := a.keys.snapshot.Load()
			return snap != nil && len(snap.keys) == 1
		}, 2*time.Second, 5*time.Millisecond)

		_, err = authenticate(t, a, "Bearer "+token())
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
		assert.EqualValues(t, 2, idp.jwksFetches.Load())
	})

	t.Run("a cancelled request does not block key set downloads", func(t *testing.T) {
		t.Parallel()

		clock := newFakeClock()
		idp := newFakeIDP(t, "rsa1")
		a := newTestAuthenticator(t, idp, clock)

		rotated := sign(t, "rsa2", idp.claims(clock, nil))

		idp.set(func(idp *fakeIDP) { idp.published = []string{"rsa2"} })
		clock.advance(2 * time.Minute)

		// The download is detached from the cancelled request, so it succeeds.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/alerts", nil)
		req.Header.Set("Authorization", "Bearer "+rotated)

		p, err := a.Authenticate(req)
		require.NoError(t, err)
		assert.Equal(t, "oidc:alertmanager", p.ID)
		assert.EqualValues(t, 2, idp.jwksFetches.Load())

		// The removed key is no longer accepted.
		clock.advance(2 * time.Minute)

		_, err = authenticate(t, a, "Bearer "+sign(t, "rsa1", idp.claims(clock, nil)))
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
	})

	t.Run("a slow identity provider does not stall requests with cached keys", func(t *testing.T) {
		t.Parallel()

		clock := newFakeClock()
		idp := newFakeIDP(t, "rsa1")
		a := newTestAuthenticator(t, idp, clock, WithJWKSMaxAge(10*time.Minute))

		gate := make(chan struct{})
		idp.set(func(idp *fakeIDP) { idp.jwksGate = gate })
		clock.advance(11 * time.Minute)

		token := sign(t, "rsa1", idp.claims(clock, nil))

		for range 3 {
			done := make(chan error, 1)

			go func() {
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/alerts", nil)
				req.Header.Set("Authorization", "Bearer "+token)

				_, err := a.Authenticate(req)
				done <- err
			}()

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("request blocked by the key set download")
			}
		}

		close(gate)

		require.Eventually(t, func() bool { return !a.keys.refreshing.Load() }, 2*time.Second, 5*time.Millisecond)
		assert.EqualValues(t, 2, idp.jwksFetches.Load(), "one background download")
	})

	t.Run("cached keys keep working while the key set is unavailable", func(t *testing.T) {
		t.Parallel()

		clock := newFakeClock()
		idp := newFakeIDP(t, "rsa1")
		a := newTestAuthenticator(t, idp, clock, WithJWKSMaxAge(10*time.Minute))

		idp.set(func(idp *fakeIDP) { idp.failJWKS = true })
		clock.advance(11 * time.Minute)

		_, err := authenticate(t, a, "Bearer "+sign(t, "rsa1", idp.claims(clock, nil)))
		require.NoError(t, err)

		// An unknown key cannot be told apart from a forged one: it is invalid.
		_, err = authenticate(t, a, "Bearer "+sign(t, "rsa2", idp.claims(clock, nil)))
		require.ErrorIs(t, err, types.ErrInvalidCredentials)
	})

	t.Run("no keys at all is an internal error", func(t *testing.T) {
		t.Parallel()

		clock := newFakeClock()
		idp := newFakeIDP(t, "rsa1")
		a := newTestAuthenticator(t, idp, clock)

		// Simulate an authenticator whose key set was never loaded.
		a.keys.snapshot.Store(nil)
		idp.set(func(idp *fakeIDP) { idp.failJWKS = true })
		clock.advance(time.Hour)

		_, err := authenticate(t, a, "Bearer "+sign(t, "rsa1", idp.claims(clock, nil)))
		require.Error(t, err)
		require.NotErrorIs(t, err, types.ErrInvalidCredentials)
		require.NotErrorIs(t, err, types.ErrNoCredentials)
		assert.True(t, IsKeySetUnavailable(err))

		// Within the min refresh interval, no new download is attempted, and it is still an internal error.
		fetches := idp.jwksFetches.Load()

		_, err = authenticate(t, a, "Bearer "+sign(t, "rsa1", idp.claims(clock, nil)))
		assert.True(t, IsKeySetUnavailable(err))
		assert.Equal(t, fetches, idp.jwksFetches.Load())
	})
}

func TestAuthenticator_Concurrent(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	idp := newFakeIDP(t, "rsa1")
	a := newTestAuthenticator(t, idp, clock)

	valid := sign(t, "rsa1", idp.claims(clock, nil))
	unknown := sign(t, "rsa2", idp.claims(clock, nil))

	clock.advance(2 * time.Minute)

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Go(func() {
			token := valid
			if i%2 == 1 {
				token = unknown
			}

			_, _ = authenticate(t, a, "Bearer "+token)
		})
	}

	wg.Wait()

	assert.LessOrEqual(t, idp.jwksFetches.Load(), int32(2), "concurrent unknown keys share one download")
}

func TestAuthenticator_ChainWithAPIKeys(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	idp := newFakeIDP(t, "rsa1")
	a := newTestAuthenticator(t, idp, clock)

	apiKeys := types.AuthenticatorFunc(func(r *http.Request) (*types.Principal, error) {
		if r.Header.Get("Authorization") == "Bearer api-key" {
			return &types.Principal{ID: "legacy"}, nil
		}

		return nil, types.ErrInvalidCredentials
	})

	chain := types.ChainAuthenticators(apiKeys, a)

	for header, wantID := range map[string]string{
		"Bearer api-key": "legacy",
		"Bearer " + sign(t, "rsa1", idp.claims(clock, nil)): "oidc:alertmanager",
	} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/alerts", nil)
		req.Header.Set("Authorization", header)

		p, err := chain.Authenticate(req)
		require.NoError(t, err)
		assert.Equal(t, wantID, p.ID)
	}
}

func TestNew(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t, "rsa1")

	t.Run("issuer and JWKS URL", func(t *testing.T) {
		t.Parallel()

		a := newTestAuthenticator(t, idp, newFakeClock())
		assert.Equal(t, idp.server.URL, a.Issuer())
		assert.Equal(t, idp.server.URL+"/jwks", a.keys.url)
	})

	t.Run("WithJWKSURL skips discovery", func(t *testing.T) {
		t.Parallel()

		other := newFakeIDP(t, "rsa1")
		other.set(func(idp *fakeIDP) { idp.discoveryCode = http.StatusNotFound })

		a, err := New(t.Context(), "https://issuer.example.com", WithAudience(testAudience), WithHTTPClient(other.server.Client()), WithJWKSURL(other.server.URL+"/jwks"))
		require.NoError(t, err)
		assert.Equal(t, other.server.URL+"/jwks", a.keys.url)
	})

	errorTests := []struct {
		name       string
		issuer     string
		noAudience bool
		setup      func(idp *fakeIDP)
		opts       []Option
		wantErr    string
	}{
		{name: "missing audience", noAudience: true, opts: []Option{WithAudience(" ")}, wantErr: "at least one audience is required"},
		{name: "http issuer", issuer: "http://login.example.com", wantErr: `issuer URL "http://login.example.com" must use https (http is only allowed for localhost)`},
		{name: "relative issuer", issuer: "/realms/ops", wantErr: "must be an absolute URL"},
		{name: "other scheme", issuer: "ftp://login.example.com", wantErr: "must use https"},
		{name: "invalid issuer URL", issuer: "https://%zz", wantErr: "issuer URL is not a valid URL"},
		{name: "http JWKS URL", opts: []Option{WithJWKSURL("http://keys.example.com/jwks")}, wantErr: "JWKS URL"},
		{name: "discovery not found", setup: func(idp *fakeIDP) { idp.discoveryCode = http.StatusNotFound }, wantErr: "failed to fetch the discovery document: unexpected status 404 Not Found"},
		{name: "issuer mismatch", setup: func(idp *fakeIDP) { idp.issuer = "https://other.example.com" }, wantErr: `the discovery document is for issuer "https://other.example.com"`},
		{name: "key set unavailable", setup: func(idp *fakeIDP) { idp.failJWKS = true }, wantErr: "failed to download the key set"},
		{name: "empty key set", setup: func(idp *fakeIDP) { idp.jwksBody = `{"keys":[]}` }, wantErr: "the key set has no public signing keys"},
		{name: "key set without public signing keys", setup: func(idp *fakeIDP) {
			idp.jwksBody = `{"keys":[{"kty":"oct","k":"c2VjcmV0","kid":"hmac"},{"kty":"RSA","use":"enc","kid":"x","n":"AQAB","e":"AQAB"},{"kty":"unknown"}]}`
		}, wantErr: "the key set has no public signing keys"},
		{name: "invalid key set", setup: func(idp *fakeIDP) { idp.jwksBody = `not json` }, wantErr: "failed to decode the response"},
		{name: "too large key set", setup: func(idp *fakeIDP) {
			idp.jwksBody = `{"keys":[],"x":"` + strings.Repeat("a", maxResponseBodySize) + `"}`
		}, wantErr: "larger than"},
		{name: "symmetric algorithm", opts: []Option{WithSigningAlgorithms("HS256")}, wantErr: `signing algorithm "HS256" is not supported`},
		{name: "none algorithm", opts: []Option{WithSigningAlgorithms("none")}, wantErr: `signing algorithm "none" is not supported`},
		{name: "no algorithms", opts: []Option{WithSigningAlgorithms()}, wantErr: "at least one signing algorithm is required"},
		{name: "no principal ID claims", opts: []Option{WithPrincipalIDClaims()}, wantErr: "principal ID claims (WithPrincipalIDClaims) cannot be empty"},
		{name: "empty principal ID claim", opts: []Option{WithPrincipalIDClaims("sub", " ")}, wantErr: "principal ID claims (WithPrincipalIDClaims) must be non-empty"},
		{name: "prefix without colon", opts: []Option{WithPrincipalIDPrefix("oidc")}, wantErr: `principal ID prefix (WithPrincipalIDPrefix) must end with ":"`},
		{name: "prefix with whitespace", opts: []Option{WithPrincipalIDPrefix("my idp:")}, wantErr: "principal ID prefix (WithPrincipalIDPrefix)"},
		{name: "too long prefix", opts: []Option{WithPrincipalIDPrefix(strings.Repeat("a", 50) + ":")}, wantErr: "at most 50 characters"},
		{name: "empty scope claim", opts: []Option{WithScopeClaims("")}, wantErr: "scope claims (WithScopeClaims) must be non-empty"},
		{name: "too long attribute claim", opts: []Option{WithAttributeClaims(strings.Repeat("a", 201))}, wantErr: "attribute claims (WithAttributeClaims) must be non-empty"},
		{name: "reserved attribute claim", opts: []Option{WithAttributeClaims("sub", "auth_method")}, wantErr: `attribute claims (WithAttributeClaims) cannot be "auth_method" or "issuer"`},
		{name: "reserved issuer attribute claim", opts: []Option{WithAttributeClaims("issuer")}, wantErr: "cannot be"},
		{name: "empty required token type", opts: []Option{WithRequiredTokenType("at+jwt", " ")}, wantErr: "required token types (WithRequiredTokenType) cannot be empty"},
		{name: "empty scope mapping", opts: []Option{WithScopeMapping(map[string]string{"a": " "})}, wantErr: "scope mapping (WithScopeMapping) cannot have empty scopes"},
		{name: "negative clock skew", opts: []Option{WithClockSkew(-time.Second)}, wantErr: "clock skew (WithClockSkew) must be between 0 and 5m0s"},
		{name: "too large clock skew", opts: []Option{WithClockSkew(6 * time.Minute)}, wantErr: "clock skew"},
		{name: "too short refresh interval", opts: []Option{WithJWKSMinRefreshInterval(time.Millisecond)}, wantErr: "JWKS min refresh interval"},
		{name: "too long refresh interval", opts: []Option{WithJWKSMinRefreshInterval(2 * time.Hour)}, wantErr: "JWKS min refresh interval"},
		{name: "too short max age", opts: []Option{WithJWKSMaxAge(time.Second)}, wantErr: "JWKS max age (WithJWKSMaxAge) must be between"},
		{name: "too long max age", opts: []Option{WithJWKSMaxAge(8 * 24 * time.Hour)}, wantErr: "JWKS max age (WithJWKSMaxAge) must be between"},
		{name: "max age below refresh interval", opts: []Option{WithJWKSMinRefreshInterval(30 * time.Minute), WithJWKSMaxAge(10 * time.Minute)}, wantErr: "must be at least the min refresh interval"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newFakeIDP(t, "rsa1")
			if tt.setup != nil {
				idp.set(tt.setup)
			}

			issuer := tt.issuer
			if issuer == "" {
				issuer = idp.server.URL
			}

			opts := append([]Option{WithHTTPClient(idp.server.Client())}, tt.opts...)
			if !tt.noAudience {
				opts = append(opts, WithAudience(testAudience))
			}

			a, err := New(t.Context(), issuer, opts...)
			require.Error(t, err)
			assert.Nil(t, a)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}

	t.Run("cancelled context", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := New(ctx, idp.server.URL, WithAudience(testAudience), WithHTTPClient(idp.server.Client()))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestValidateURL(t *testing.T) {
	t.Parallel()

	for _, u := range []string{"https://login.example.com", "http://localhost:8080/realms/dev", "http://127.0.0.1:9000", "http://[::1]:9000"} {
		require.NoError(t, validateURL("issuer URL", u), u)
	}

	for _, u := range []string{"http://login.example.com", "http://10.0.0.1", "https://", "login.example.com"} {
		require.Error(t, validateURL("issuer URL", u), u)
	}
}

func TestOptions(t *testing.T) {
	t.Parallel()

	o := newOptions()
	defaultClient := o.httpClient

	WithHTTPClient(nil)(o)
	WithLogger(nil)(o)
	assert.Same(t, defaultClient, o.httpClient)
	assert.IsType(t, &types.NoopLogger{}, o.logger)

	logger := &types.NoopLogger{}
	WithLogger(logger)(o)
	assert.Same(t, logger, o.logger)

	WithAudience("a", " b ", "")(o)
	WithAudience("c")(o)
	assert.Equal(t, []string{"a", "b", "c"}, o.audiences)

	WithSigningAlgorithms(" ES384 ")(o)
	assert.Equal(t, []jose.SignatureAlgorithm{jose.ES384}, o.algorithms)
	require.NoError(t, o.validate())

	assert.False(t, IsKeySetUnavailable(errors.New("other")))
}
