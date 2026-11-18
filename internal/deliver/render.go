package deliver

import (
	"bytes"
	"embed"
	"fmt"
	htmltpl "html/template"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/textproto"
	"net/url"
	"regexp"
	"sort"
	"strings"
	texttpl "text/template"
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/unsub"
)

// Site is how a site's emails look and link.
type Site struct {
	Name       string `json:"name"`        // "Mert Metin, REALTOR®"
	BaseURL    string `json:"base_url"`    // "https://mertmetin.ca"
	ListingURL string `json:"listing_url"` // template: {key}, {slug}
	ManageURL  string `json:"manage_url"`  // where users manage saved searches
	SignInURL  string `json:"sign_in_url"` // for "sign in to see the sold price"
	Phone      string `json:"phone,omitempty"`
}

// DefaultSites are the two production sites.
var DefaultSites = map[string]Site{
	"mertmetin": {
		Name: "Mert Metin, REALTOR®", BaseURL: "https://mertmetin.ca",
		ListingURL: "https://mertmetin.ca/homes-for-sale/{slug}/",
		ManageURL:  "https://mertmetin.ca/my-account/", SignInURL: "https://mertmetin.ca/my-account/",
	},
	"hotrentals": {
		Name: "HotRentals.ca", BaseURL: "https://hotrentals.ca",
		ListingURL: "https://hotrentals.ca/listing/{key}",
		ManageURL:  "https://hotrentals.ca/account", SignInURL: "https://hotrentals.ca/account",
	},
}

// Renderer builds messages from alerts.
type Renderer struct {
	Sites       map[string]Site
	UnsubURL    string // e.g. "https://alerts.mertmetin.ca/v1/unsubscribe"
	UnsubSecret []byte
	Domain      string // for Message-IDs, e.g. "mertmetin.ca"
}

//go:embed templates/*
var templates embed.FS

var (
	htmlT = htmltpl.Must(htmltpl.New("alert.html").Funcs(htmltpl.FuncMap{"money": money}).ParseFS(templates, "templates/alert.html"))
	textT = texttpl.Must(texttpl.New("alert.txt").Funcs(texttpl.FuncMap{"money": money}).ParseFS(templates, "templates/alert.txt"))
)

// card is one listing in an email.
type card struct {
	Headline   string // "Price drop", "New listing", ...
	Title      string // address, or community and city when withheld
	Price      string
	Detail     string // "$709,900 (was $719,900, down $10,000)"
	Facts      string // "3 bd · 2 ba · Detached · Barrhaven, Ottawa"
	Brokerage  string
	URL        string
	SearchName string
	Unsub      string
	SignIn     string // set when a sold price is withheld from a public user
}

type view struct {
	Site     Site
	Intro    string
	Cards    []card
	Manage   string
	UnsubAll string
}

// Render builds one message for a user's alerts (one or many: a digest).
func (r Renderer) Render(alerts []event.Alert) (Message, error) {
	if len(alerts) == 0 {
		return Message{}, fmt.Errorf("no alerts")
	}
	sort.SliceStable(alerts, func(i, j int) bool { return alerts[i].MatchedAt.Before(alerts[j].MatchedAt) })
	first := alerts[0]
	site, ok := r.Sites[first.Site]
	if !ok {
		return Message{}, PermanentError{fmt.Errorf("unknown site %q", first.Site)}
	}
	v := view{Site: site, Manage: site.ManageURL}
	v.UnsubAll = r.unsubLink(unsub.Target{Scope: unsub.User, Site: first.Site, ID: first.UserID})
	for _, a := range alerts {
		v.Cards = append(v.Cards, r.card(a, site))
	}

	m := Message{To: first.Email, MessageID: r.messageID(alerts)}
	if len(alerts) == 1 {
		c := v.Cards[0]
		m.Subject = c.Headline + ": " + c.Title
		if c.Price != "" {
			m.Subject += " · " + c.Price
		}
		v.Intro = "A home matching your saved search “" + c.SearchName + "”."
		m.SMS = m.Subject + " " + c.URL
	} else {
		m.Subject = fmt.Sprintf("%d updates for your saved searches", len(alerts))
		v.Intro = fmt.Sprintf("%d homes matching your saved searches changed recently.", len(alerts))
		m.SMS = m.Subject + " " + site.ManageURL
	}
	var h, t bytes.Buffer
	if err := htmlT.Execute(&h, v); err != nil {
		return Message{}, err
	}
	if err := textT.Execute(&t, v); err != nil {
		return Message{}, err
	}
	m.HTML, m.Text = h.String(), t.String()
	m.Headers = map[string]string{
		// RFC 8058 one-click unsubscribe; Gmail requires it of bulk senders.
		"List-Unsubscribe":      "<" + v.UnsubAll + ">",
		"List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
		"X-Alert-Count":         fmt.Sprint(len(alerts)),
	}
	return m, nil
}

func (r Renderer) card(a event.Alert, site Site) card {
	l := a.Listing
	c := card{SearchName: a.SearchName, Brokerage: l.ListOffice}
	if c.SearchName == "" {
		c.SearchName = "your saved search"
	}
	street := strings.TrimSpace(strings.Join(nonEmpty(l.StreetNumber, l.StreetName, l.StreetSuffix), " "))
	if l.Unit != "" && street != "" {
		street = l.Unit + " - " + street
	}
	place := strings.Join(nonEmpty(l.Community, cityOf(l.City, l.County)), ", ")
	switch {
	case street != "" && (l.AddressDisplay || a.Audience == "vow"):
		c.Title = street + ", " + cityOf(l.City, l.County)
	case place != "":
		c.Title = "Home in " + place
	default:
		c.Title = "Listing " + a.ListingKey
	}
	lease := l.TransactionType != "For Sale"
	switch a.Type {
	case event.NewListing:
		c.Headline = "New listing"
		if lease {
			c.Headline = "New rental"
		}
		c.Price = money(a.Price)
	case event.PriceDrop:
		c.Headline = "Price drop"
		c.Price = money(a.Price)
		if a.PrevPrice != nil && a.Price != nil {
			c.Detail = fmt.Sprintf("Now %s, was %s (down %s)", money(a.Price), money(a.PrevPrice), money(ptr(*a.PrevPrice-*a.Price)))
		}
	case event.Sold, event.Leased:
		c.Headline = "Sold"
		if a.Type == event.Leased {
			c.Headline = "Leased"
		}
		if a.ClosePrice != nil { // vow audience only: the matcher strips it otherwise
			c.Price = money(a.ClosePrice)
			c.Detail = fmt.Sprintf("%s for %s, listed at %s", c.Headline, money(a.ClosePrice), money(a.Price))
		} else {
			c.Detail = "Listed at " + money(a.Price) + "."
			c.SignIn = site.SignInURL
		}
	case event.BackOnMarket:
		c.Headline = "Back on the market"
		c.Price = money(a.Price)
	case event.Pending:
		c.Headline = "Conditionally sold"
		if lease {
			c.Headline = "Conditionally leased"
		}
	case event.OffMarket:
		c.Headline = "Off the market"
	default:
		c.Headline = "Update"
	}
	var facts []string
	if l.Bedrooms != nil {
		facts = append(facts, fmt.Sprintf("%d bd", *l.Bedrooms))
	}
	if l.Bathrooms != nil {
		facts = append(facts, fmt.Sprintf("%d ba", *l.Bathrooms))
	}
	facts = append(facts, nonEmpty(l.PropertySubType, place)...)
	c.Facts = strings.Join(facts, " · ")
	c.URL = listingURL(site.ListingURL, a.ListingKey, street)
	c.Unsub = r.unsubLink(unsub.Target{Scope: unsub.Search, Site: a.Site, ID: a.SearchID})
	return c
}

func (r Renderer) unsubLink(t unsub.Target) string {
	return r.UnsubURL + "?t=" + url.QueryEscape(unsub.Sign(r.UnsubSecret, t))
}

// messageID is derived from the alert IDs: a resend after a crash carries
// the same Message-ID, which Gmail uses to drop the duplicate.
func (r Renderer) messageID(alerts []event.Alert) string {
	ids := make([]string, len(alerts))
	for i, a := range alerts {
		ids[i] = a.ID
	}
	sort.Strings(ids)
	sum := event.AlertID(strings.Join(ids, ","), "msg")
	return "<" + sum + "@" + r.Domain + ">"
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func listingURL(tpl, key, street string) string {
	slug := strings.ToLower(key)
	if s := strings.Trim(slugRe.ReplaceAllString(strings.ToLower(street), "-"), "-"); s != "" {
		slug += "-" + s
	}
	return strings.NewReplacer("{key}", url.PathEscape(key), "{slug}", slug).Replace(tpl)
}

// cityOf drops a Toronto-style district code ("Toronto W08" -> "Toronto")
// and prefers the county for neighbourhood names ("Barrhaven" -> keep, it
// reads well; the county is added by the caller when needed).
func cityOf(city, county string) string {
	if m := regexp.MustCompile(`^(.+?)\s+[A-Z]\d{2}$`).FindStringSubmatch(city); m != nil {
		return m[1]
	}
	if city == "" {
		return county
	}
	return city
}

func money(v *float64) string {
	if v == nil {
		return ""
	}
	n := int64(*v + 0.5)
	s := fmt.Sprint(n)
	var b strings.Builder
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(ch)
	}
	return "$" + b.String()
}

func ptr(f float64) *float64 { return &f }

func nonEmpty(ss ...string) []string {
	var out []string
	for _, s := range ss {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// buildMIME writes a multipart/alternative message (text + HTML).
func buildMIME(from string, m Message) []byte {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, part := range []struct{ ctype, content string }{{"text/plain; charset=utf-8", m.Text}, {"text/html; charset=utf-8", m.HTML}} {
		w, _ := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {part.ctype}, "Content-Transfer-Encoding": {"quoted-printable"}})
		qp := quotedprintable.NewWriter(w)
		qp.Write([]byte(part.content))
		qp.Close()
	}
	mw.Close()

	var out bytes.Buffer
	h := func(k, v string) { fmt.Fprintf(&out, "%s: %s\r\n", k, v) }
	h("From", encodeAddr(from))
	h("To", m.To)
	h("Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	h("Date", time.Now().UTC().Format(time.RFC1123Z))
	h("Message-ID", m.MessageID)
	h("MIME-Version", "1.0")
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h(k, m.Headers[k])
	}
	h("Content-Type", "multipart/alternative; boundary="+mw.Boundary())
	out.WriteString("\r\n")
	out.Write(body.Bytes())
	return out.Bytes()
}

func encodeAddr(a string) string {
	if i := strings.LastIndex(a, "<"); i > 0 {
		return mime.QEncoding.Encode("utf-8", strings.TrimSpace(a[:i])) + " " + a[i:]
	}
	return a
}
