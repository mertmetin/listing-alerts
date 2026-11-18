// Package unsub signs and checks unsubscribe tokens. A token names either
// one saved search or every search of one user on one site, and carries an
// HMAC so links cannot be forged. Tokens do not expire: an unsubscribe link
// in an old email must keep working.
package unsub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

// Scope of a token.
type Scope string

const (
	Search Scope = "s" // one saved search
	User   Scope = "u" // all of a user's searches on a site
)

// Target is what a token unsubscribes.
type Target struct {
	Scope Scope
	Site  string
	ID    string // search ID (Search) or user ID (User)
}

var enc = base64.RawURLEncoding

// Sign returns a token for t.
func Sign(secret []byte, t Target) string {
	body := enc.EncodeToString([]byte(string(t.Scope) + "\x00" + t.Site + "\x00" + t.ID))
	return body + "." + enc.EncodeToString(mac(secret, body))
}

// Verify checks a token and returns its target.
func Verify(secret []byte, token string) (Target, error) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok {
		return Target{}, errors.New("malformed token")
	}
	got, err := enc.DecodeString(sig)
	if err != nil || !hmac.Equal(got, mac(secret, body)) {
		return Target{}, errors.New("invalid token")
	}
	raw, err := enc.DecodeString(body)
	if err != nil {
		return Target{}, errors.New("malformed token")
	}
	parts := strings.Split(string(raw), "\x00")
	if len(parts) != 3 || (parts[0] != string(Search) && parts[0] != string(User)) || parts[1] == "" || parts[2] == "" {
		return Target{}, errors.New("malformed token")
	}
	return Target{Scope: Scope(parts[0]), Site: parts[1], ID: parts[2]}, nil
}

func mac(secret []byte, body string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte("listing-alerts/unsub/v1\x00" + body))
	return h.Sum(nil)[:20]
}
