package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	// maxResponseBodySize is the maximum size of a discovery document or key set.
	maxResponseBodySize = 1 << 20

	wellKnownConfigurationPath = "/.well-known/openid-configuration"
)

// discoveryDocument holds the fields of an OIDC discovery document used by the authenticator.
type discoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURL string `json:"jwks_uri"` //nolint:tagliatelle // Field name defined by OpenID Connect Discovery
}

// discoverJWKSURL fetches the discovery document of the issuer, checks that it is for the issuer, and returns
// its jwks_uri.
func discoverJWKSURL(ctx context.Context, client *http.Client, issuer string) (string, error) {
	var doc discoveryDocument

	if err := getJSON(ctx, client, strings.TrimSuffix(issuer, "/")+wellKnownConfigurationPath, &doc); err != nil {
		return "", fmt.Errorf("failed to fetch the discovery document: %w", err)
	}

	if doc.Issuer != issuer {
		return "", fmt.Errorf("the discovery document is for issuer %q, expected %q", doc.Issuer, issuer)
	}

	if doc.JWKSURL == "" {
		return "", errors.New("the discovery document has no jwks_uri")
	}

	return doc.JWKSURL, nil
}

// getJSON fetches a JSON document with a GET request, and decodes it into v.
func getJSON(ctx context.Context, client *http.Client, rawURL string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req) // #nosec G704 -- the URL is the configured issuer, its jwks_uri, or WithJWKSURL
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize+1))
	if err != nil {
		return fmt.Errorf("failed to read the response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	if len(body) > maxResponseBodySize {
		return fmt.Errorf("the response is larger than %d bytes", maxResponseBodySize)
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("failed to decode the response: %w", err)
	}

	return nil
}

// validateURL checks that rawURL is an absolute https URL, or an http URL of a loopback host (for local
// development). Tokens and keys must not be fetched over plain HTTP from other hosts.
func validateURL(what, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s is not a valid URL: %w", what, err)
	}

	if u.Host == "" {
		return fmt.Errorf("%s %q must be an absolute URL", what, rawURL)
	}

	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopback(u.Hostname()) {
			return nil
		}

		return fmt.Errorf("%s %q must use https (http is only allowed for localhost)", what, rawURL)
	default:
		return fmt.Errorf("%s %q must use https", what, rawURL)
	}
}

// maxRedirects is the maximum number of redirects followed by the default HTTP client.
const maxRedirects = 5

// checkRedirect is the redirect policy of the default HTTP client: it only follows redirects to URLs that pass
// validateURL, so that a redirect cannot downgrade a key set download to plain HTTP.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}

	return validateURL("redirect URL", req.URL.String())
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}
