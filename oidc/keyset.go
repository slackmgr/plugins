package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/slackmgr/types"
)

// errKeysUnavailable means that no key set could be downloaded, so tokens cannot be verified.
var errKeysUnavailable = errors.New("the key set of the identity provider is unavailable")

// keySnapshot is an immutable set of signing keys, as downloaded at fetched.
type keySnapshot struct {
	keys    []jose.JSONWebKey
	fetched time.Time
}

// keySet caches the signing keys of the identity provider. Unlike a plain JWKS cache, it limits how often tokens
// that the cached keys cannot verify (an unknown key ID or an invalid signature) trigger a download, so that
// forged tokens cannot make every request call the identity provider.
type keySet struct {
	url        string
	client     *http.Client
	minRefresh time.Duration
	maxAge     time.Duration
	now        func() time.Time
	logger     types.Logger

	snapshot atomic.Pointer[keySnapshot]

	mu          sync.Mutex // serializes downloads, and guards lastAttempt
	lastAttempt time.Time

	refreshing atomic.Bool // a background refresh is running
}

// fetchTimeout limits a key set download, in addition to the timeout of the HTTP client.
const fetchTimeout = 10 * time.Second

func newKeySet(url string, o *options) *keySet {
	return &keySet{
		url:        url,
		client:     o.httpClient,
		minRefresh: o.jwksMinRefresh,
		maxAge:     o.jwksMaxAge,
		now:        o.now,
		logger:     o.logger,
	}
}

// candidates returns the cached keys that may have signed a token with the given key ID and algorithm:
//   - The key ID must match, if the token has one. If no key has it, keys without a key ID are returned
//   - The algorithm of the key must match, if the key has one
//
// Without cached keys, it downloads the key set and waits for it. With refresh, it forces a download and waits for
// it, unless the last one was attempted less than the min refresh interval ago. If the cached key set is older
// than the max age, it is refreshed in the background, and the cached keys are used meanwhile, so that a slow
// identity provider does not stall requests.
//
// It returns errKeysUnavailable if there are no cached keys and the download fails.
func (k *keySet) candidates(ctx context.Context, keyID, algorithm string, refresh bool) ([]jose.JSONWebKey, error) {
	snap := k.snapshot.Load()

	switch {
	case snap == nil:
		var err error

		if snap, err = k.refresh(ctx, nil); snap == nil {
			if err == nil {
				return nil, errKeysUnavailable
			}

			return nil, fmt.Errorf("%w: %w", errKeysUnavailable, err)
		}
	case refresh:
		snap, _ = k.refresh(ctx, snap)
	case k.now().Sub(snap.fetched) >= k.maxAge:
		k.refreshInBackground(snap) //nolint:contextcheck // The background refresh must outlive the request.
	}

	return matchingKeys(snap.keys, keyID, algorithm), nil
}

func matchingKeys(keys []jose.JSONWebKey, keyID, algorithm string) []jose.JSONWebKey {
	byID := make([]jose.JSONWebKey, 0, 1)
	withoutID := make([]jose.JSONWebKey, 0, 1)

	for _, key := range keys {
		if key.Algorithm != "" && key.Algorithm != algorithm {
			continue
		}

		switch {
		case keyID == "" || key.KeyID == keyID:
			byID = append(byID, key)
		case key.KeyID == "":
			withoutID = append(withoutID, key)
		}
	}

	if len(byID) > 0 {
		return byID
	}

	return withoutID
}

// refreshInBackground starts a download of the key set in a goroutine, unless one is already running.
func (k *keySet) refreshInBackground(seen *keySnapshot) {
	if !k.refreshing.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer k.refreshing.Store(false)

		_, _ = k.refresh(context.Background(), seen)
	}()
}

// refresh downloads the key set, unless a download was attempted less than the min refresh interval ago (or
// another goroutine downloaded it while this one waited). It returns the current snapshot, which is the old one
// if the download failed or was skipped.
//
// The download is detached from the cancellation of ctx (and limited by fetchTimeout instead): otherwise a caller
// that cancels its request could make the download fail, and block downloads for the min refresh interval.
func (k *keySet) refresh(ctx context.Context, seen *keySnapshot) (*keySnapshot, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Another goroutine replaced the snapshot while this one waited for the lock.
	if current := k.snapshot.Load(); current != seen {
		return current, nil
	}

	now := k.now()

	if !k.lastAttempt.IsZero() && now.Sub(k.lastAttempt) < k.minRefresh {
		return seen, nil
	}

	k.lastAttempt = now

	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
	defer cancel()

	keys, err := k.fetch(fetchCtx)
	if err != nil {
		k.logger.WithField("jwks_url", k.url).WithField("error", err.Error()).Error("Failed to download the OIDC key set")
		return seen, err
	}

	snap := &keySnapshot{keys: keys, fetched: now}
	k.snapshot.Store(snap)

	k.logger.WithField("jwks_url", k.url).WithField("keys", len(keys)).Info("Downloaded the OIDC key set")

	return snap, nil
}

// fetch downloads the key set, and returns its public signing keys. Keys of unsupported types, private or
// symmetric keys, and encryption keys are skipped.
func (k *keySet) fetch(ctx context.Context) ([]jose.JSONWebKey, error) {
	var raw struct {
		Keys []json.RawMessage `json:"keys"`
	}

	if err := getJSON(ctx, k.client, k.url, &raw); err != nil {
		return nil, err
	}

	keys := make([]jose.JSONWebKey, 0, len(raw.Keys))

	for _, data := range raw.Keys {
		var key jose.JSONWebKey

		if err := key.UnmarshalJSON(data); err != nil {
			continue // unsupported key type or algorithm
		}

		if !key.Valid() || !key.IsPublic() || (key.Use != "" && key.Use != "sig") {
			continue
		}

		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return nil, errors.New("the key set has no public signing keys")
	}

	return keys, nil
}
