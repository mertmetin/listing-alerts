package deliver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/unsub"
)

var t0 = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func f(v float64) *float64 { return &v }

func alert(id, user string, typ event.Type, audience string) event.Alert {
	beds, baths := 3, 2
	return event.Alert{
		ID: id, Site: "mertmetin", UserID: user, Email: user + "@example.com", Audience: audience,
		SearchID: "ss_1", SearchName: "Orleans under $800K", EventID: "e-" + id, Type: typ, ListingKey: "X12345678",
		Price: f(709900), PrevPrice: f(719900), ClosePrice: f(712000),
		Listing: listing.Listing{Key: "X12345678", TransactionType: "For Sale", StreetNumber: "123", StreetName: "Main",
			StreetSuffix: "Street", City: "Orleans", County: "Ottawa", PropertySubType: "Detached",
			Bedrooms: &beds, Bathrooms: &baths, ListOffice: "EXAMPLE REALTY", AddressDisplay: true},
		ObservedAt: t0, MatchedAt: t0,
	}
}

func renderer() Renderer {
	return Renderer{Sites: DefaultSites, UnsubURL: "https://alerts.example/v1/unsubscribe", UnsubSecret: []byte("s3cret"), Domain: "mertmetin.ca"}
}

func TestRenderSingle(t *testing.T) {
	m, err := renderer().Render([]event.Alert{alert("a1", "u1", event.PriceDrop, "vow")})
	if err != nil {
		t.Fatal(err)
	}
	if m.Subject != "Price drop: 123 Main Street, Orleans · $709,900" {
		t.Errorf("subject %q", m.Subject)
	}
	for _, want := range []string{"Now $709,900, was $719,900 (down $10,000)", "Listing brokerage: EXAMPLE REALTY",
		"https://mertmetin.ca/homes-for-sale/x12345678-123-main-street/", "Orleans under $800K", "3 bd · 2 ba · Detached"} {
		if !strings.Contains(m.Text, want) || !strings.Contains(m.HTML, strings.ReplaceAll(want, "$", "$")) {
			t.Errorf("message missing %q", want)
		}
	}
	if !strings.HasPrefix(m.Headers["List-Unsubscribe"], "<https://alerts.example/v1/unsubscribe?t=") || m.Headers["List-Unsubscribe-Post"] != "List-Unsubscribe=One-Click" {
		t.Errorf("unsubscribe headers %v", m.Headers)
	}
	again, _ := renderer().Render([]event.Alert{alert("a1", "u1", event.PriceDrop, "vow")})
	if again.MessageID != m.MessageID {
		t.Error("Message-ID is not deterministic")
	}
}

func TestRenderSoldByAudience(t *testing.T) {
	vow, _ := renderer().Render([]event.Alert{alert("a1", "u1", event.Sold, "vow")})
	if !strings.Contains(vow.Text, "Sold for $712,000, listed at $709,900") {
		t.Errorf("vow sold text:\n%s", vow.Text)
	}
	pub := alert("a2", "u2", event.Sold, "public")
	pub.ClosePrice = nil // the matcher strips it for public users
	m, _ := renderer().Render([]event.Alert{pub})
	if strings.Contains(m.Text, "712,000") || !strings.Contains(m.Text, "Sign in to see the sold price") {
		t.Errorf("public sold text:\n%s", m.Text)
	}
}

func TestRenderWithheldAddress(t *testing.T) {
	a := alert("a1", "u1", event.NewListing, "public")
	a.Listing.AddressDisplay = false
	a.Listing.StreetNumber, a.Listing.StreetName, a.Listing.Community = "", "", "Convent Glen"
	m, _ := renderer().Render([]event.Alert{a})
	if strings.Contains(m.Text, "Main") || !strings.Contains(m.Subject, "Home in Convent Glen, Orleans") {
		t.Errorf("subject %q text %s", m.Subject, m.Text)
	}
}

func TestRenderDigestAndMIME(t *testing.T) {
	m, _ := renderer().Render([]event.Alert{alert("a1", "u1", event.NewListing, "vow"), alert("a2", "u1", event.PriceDrop, "vow"), alert("a3", "u1", event.Sold, "vow")})
	if m.Subject != "3 updates for your saved searches" || strings.Count(m.Text, "Stop this search") != 3 {
		t.Errorf("digest %q", m.Subject)
	}
	raw := string(buildMIME("Mert Metin <alerts@mertmetin.ca>", m))
	for _, want := range []string{"Message-ID: " + m.MessageID, "List-Unsubscribe-Post: List-Unsubscribe=One-Click", "multipart/alternative", "text/html; charset=utf-8"} {
		if !strings.Contains(raw, want) {
			t.Errorf("MIME missing %q", want)
		}
	}
}

func TestUnsubTokens(t *testing.T) {
	secret := []byte("k")
	tok := unsub.Sign(secret, unsub.Target{Scope: unsub.Search, Site: "mertmetin", ID: "ss_1"})
	got, err := unsub.Verify(secret, tok)
	if err != nil || got.ID != "ss_1" || got.Site != "mertmetin" || got.Scope != unsub.Search {
		t.Fatalf("verify %+v %v", got, err)
	}
	if _, err := unsub.Verify([]byte("other"), tok); err == nil {
		t.Error("token verified with the wrong secret")
	}
	forged := unsub.Sign(secret, unsub.Target{Scope: unsub.User, Site: "mertmetin", ID: "u9"})
	body, _, _ := strings.Cut(forged, ".")
	_, sig, _ := strings.Cut(tok, ".")
	if _, err := unsub.Verify(secret, body+"."+sig); err == nil {
		t.Error("swapped body accepted")
	}
}

func TestBackoff(t *testing.T) {
	for n, want := range map[int]time.Duration{1: time.Minute, 3: 30 * time.Minute, 5: 6 * time.Hour, 9: 6 * time.Hour} {
		if d := Backoff(n); d < want*9/10 || d > want*11/10 {
			t.Errorf("Backoff(%d) = %v, want about %v", n, d, want)
		}
	}
}

func TestClassify(t *testing.T) {
	if !IsPermanent(classify(&textproto.Error{Code: 550, Msg: "5.1.1 The email account that you tried to reach does not exist"})) {
		t.Error("bad recipient should be permanent")
	}
	if IsPermanent(classify(&textproto.Error{Code: 550, Msg: "5.4.5 Daily user sending quota exceeded"})) {
		t.Error("quota should be temporary")
	}
	if IsPermanent(classify(&textproto.Error{Code: 421, Msg: "4.7.0 Try again later"})) {
		t.Error("4xx should be temporary")
	}
}

// ---- sender with fakes ----

type fakeStore struct {
	mu      sync.Mutex
	pending []Item
	sent    map[string]string
	retry   map[string]time.Time
	deferd  map[string]time.Time
	failed  map[string]string
}

func newFakeStore(items ...Item) *fakeStore {
	return &fakeStore{pending: items, sent: map[string]string{}, retry: map[string]time.Time{}, deferd: map[string]time.Time{}, failed: map[string]string{}}
}
func (s *fakeStore) Enqueue(context.Context, []event.Alert, time.Time) (int, error) { return 0, nil }
func (s *fakeStore) Claim(_ context.Context, n int, _ time.Duration) ([]Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n = min(n, len(s.pending))
	out := s.pending[:n]
	s.pending = s.pending[n:]
	return out, nil
}
func (s *fakeStore) MarkSent(_ context.Context, ids []string, mid string) error {
	for _, id := range ids {
		s.sent[id] = mid
	}
	return nil
}
func (s *fakeStore) Retry(_ context.Context, ids []string, at time.Time, _ string) error {
	for _, id := range ids {
		s.retry[id] = at
	}
	return nil
}
func (s *fakeStore) Defer(_ context.Context, ids []string, at time.Time) error {
	for _, id := range ids {
		s.deferd[id] = at
	}
	return nil
}
func (s *fakeStore) MarkFailed(_ context.Context, ids []string, cause string) error {
	for _, id := range ids {
		s.failed[id] = cause
	}
	return nil
}
func (s *fakeStore) Reap(context.Context) (int, error) { return 0, nil }

type fakeLimiter struct {
	tokens map[string]int
	err    error
}

func (l *fakeLimiter) Take(_ context.Context, key string, burst int, every time.Duration) (bool, time.Duration, error) {
	if l.err != nil {
		return false, 0, l.err
	}
	if _, ok := l.tokens[key]; !ok {
		l.tokens[key] = burst
	}
	if l.tokens[key] == 0 {
		return false, every, nil
	}
	l.tokens[key]--
	return true, 0, nil
}

func (l *fakeLimiter) Refund(_ context.Context, key string, burst int) error {
	if l.tokens[key] < burst {
		l.tokens[key]++
	}
	return nil
}

type fakeChannel struct {
	msgs []Message
	errs []error // returned in order, then nil
}

func (c *fakeChannel) Name() string { return "email" }
func (c *fakeChannel) Send(_ context.Context, m Message) error {
	if len(c.errs) > 0 {
		err := c.errs[0]
		c.errs = c.errs[1:]
		return err
	}
	c.msgs = append(c.msgs, m)
	return nil
}

type fakeDLQ struct {
	items []Item
	err   error
}

func (d *fakeDLQ) Publish(_ context.Context, items []Item, _ string) error {
	if d.err != nil {
		return d.err
	}
	d.items = append(d.items, items...)
	return nil
}

func sender(st Store, ch Channel, dlq DeadLetters, lim Limiter) *Sender {
	return &Sender{Store: st, Limiter: lim, Channel: ch, Render: renderer(), DLQ: dlq,
		Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return t0 }, Burst: 2, Every: 10 * time.Minute, MaxPerEmail: 25, MaxAttempts: 5, Lease: time.Minute, Batch: 100}
}

func items(user string, n, attempts int) []Item {
	var out []Item
	for i := range n {
		id := fmt.Sprintf("%s-%d", user, i)
		out = append(out, Item{AlertID: id, Attempts: attempts, Alert: alert(id, user, event.NewListing, "vow")})
	}
	return out
}

func TestSenderOneEmailPerUser(t *testing.T) {
	st := newFakeStore(append(items("u1", 3, 0), items("u2", 1, 0)...)...)
	ch := &fakeChannel{}
	s := sender(st, ch, &fakeDLQ{}, &fakeLimiter{tokens: map[string]int{}})
	if _, err := s.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ch.msgs) != 2 || len(st.sent) != 4 {
		t.Fatalf("%d emails, %d sent", len(ch.msgs), len(st.sent))
	}
	if ch.msgs[0].Subject != "3 updates for your saved searches" || ch.msgs[0].To != "u1@example.com" {
		t.Errorf("first email %q to %s", ch.msgs[0].Subject, ch.msgs[0].To)
	}
}

func TestSenderRateLimitDefersWithoutCountingAttempts(t *testing.T) {
	st := newFakeStore(items("u1", 1, 0)...)
	lim := &fakeLimiter{tokens: map[string]int{"user:mertmetin:u1": 0}}
	s := sender(st, &fakeChannel{}, &fakeDLQ{}, lim)
	_, _ = s.Step(context.Background())
	if at, ok := st.deferd["u1-0"]; !ok || !at.Equal(t0.Add(10*time.Minute)) || len(st.retry) != 0 {
		t.Fatalf("deferred %v, retried %v", st.deferd, st.retry)
	}
	// Limiter unavailable: hold for 30 s rather than send unthrottled.
	st2 := newFakeStore(items("u1", 1, 0)...)
	ch := &fakeChannel{}
	_, _ = sender(st2, ch, &fakeDLQ{}, &fakeLimiter{err: errors.New("redis down")}).Step(context.Background())
	if len(ch.msgs) != 0 || !st2.deferd["u1-0"].Equal(t0.Add(30*time.Second)) {
		t.Fatalf("limiter down: sent %d, deferred %v", len(ch.msgs), st2.deferd)
	}
}

func TestSenderRetriesTemporaryFailures(t *testing.T) {
	st := newFakeStore(items("u1", 1, 2)...) // third attempt
	s := sender(st, &fakeChannel{errs: []error{errors.New("connection refused")}}, &fakeDLQ{}, &fakeLimiter{tokens: map[string]int{}})
	_, _ = s.Step(context.Background())
	at, ok := st.retry["u1-0"]
	if !ok || at.Sub(t0) < 27*time.Minute || at.Sub(t0) > 33*time.Minute {
		t.Fatalf("retry at %v (want about 30 min)", at.Sub(t0))
	}
}

func TestSenderDeadLetters(t *testing.T) {
	// Permanent error: straight to the DLQ.
	st, dlq := newFakeStore(items("u1", 2, 0)...), &fakeDLQ{}
	perm := PermanentError{&textproto.Error{Code: 550, Msg: "no such user"}}
	_, _ = sender(st, &fakeChannel{errs: []error{perm}}, dlq, &fakeLimiter{tokens: map[string]int{}}).Step(context.Background())
	if len(dlq.items) != 2 || len(st.failed) != 2 {
		t.Fatalf("permanent: dlq %d, failed %d", len(dlq.items), len(st.failed))
	}
	// Out of attempts: DLQ too.
	st, dlq = newFakeStore(items("u2", 1, 4)...), &fakeDLQ{}
	_, _ = sender(st, &fakeChannel{errs: []error{errors.New("timeout")}}, dlq, &fakeLimiter{tokens: map[string]int{}}).Step(context.Background())
	if len(dlq.items) != 1 || len(st.failed) != 1 {
		t.Fatalf("exhausted: dlq %d, failed %d", len(dlq.items), len(st.failed))
	}
	// DLQ unreachable: keep the alert retryable instead of losing it.
	st, dlq = newFakeStore(items("u3", 1, 0)...), &fakeDLQ{err: errors.New("broker down")}
	_, _ = sender(st, &fakeChannel{errs: []error{perm}}, dlq, &fakeLimiter{tokens: map[string]int{}}).Step(context.Background())
	if len(st.failed) != 0 || len(st.retry) != 1 {
		t.Fatalf("dlq down: failed %v, retry %v", st.failed, st.retry)
	}
}
