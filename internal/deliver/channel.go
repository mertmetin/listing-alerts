package deliver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"
)

// Message is one rendered notification.
type Message struct {
	To        string
	Subject   string
	Text      string
	HTML      string
	MessageID string            // deterministic, so a resend is recognisable
	Headers   map[string]string // e.g. List-Unsubscribe
	SMS       string            // short form for SMS/push channels
}

// Channel sends messages: email today, SMS or push later.
type Channel interface {
	Name() string
	Send(ctx context.Context, m Message) error
}

// PermanentError means retrying cannot help (bad address, rejected content).
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

// IsPermanent reports whether err should go straight to the dead letters.
func IsPermanent(err error) bool {
	var p PermanentError
	return errors.As(err, &p)
}

// SMTP sends email through an SMTP server: Gmail (smtp.gmail.com:587 with
// STARTTLS and an app password) in production, Mailpit in development.
type SMTP struct {
	Host     string
	Port     int
	Username string // empty: no auth (Mailpit)
	Password string
	From     string // "Mert Metin <alerts@example.com>"
	Timeout  time.Duration
}

func (s SMTP) Name() string { return "email" }

func (s SMTP) Send(ctx context.Context, m Message) error {
	timeout := s.Timeout
	if timeout == 0 {
		timeout = 20 * time.Second
	}
	if d, ok := ctx.Deadline(); ok && time.Until(d) < timeout {
		timeout = time.Until(d)
	}
	addr := net.JoinHostPort(s.Host, fmt.Sprint(s.Port))
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err // temporary: server down or unreachable
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	c, err := smtp.NewClient(conn, s.Host)
	if err != nil {
		conn.Close()
		return err
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(&tls.Config{ServerName: s.Host, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if s.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", s.Username, s.Password, s.Host)); err != nil {
			return classify(err)
		}
	}
	from := s.From
	if i := strings.LastIndex(from, "<"); i >= 0 {
		from = strings.Trim(from[i:], "<>")
	}
	if err := c.Mail(from); err != nil {
		return classify(err)
	}
	if err := c.Rcpt(m.To); err != nil {
		return classify(err)
	}
	w, err := c.Data()
	if err != nil {
		return classify(err)
	}
	if _, err := w.Write(buildMIME(s.From, m)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return classify(err)
	}
	return c.Quit()
}

// classify: SMTP 5xx replies are permanent, except 552/554 policy blocks
// that Gmail also uses for rate limiting ("try again later").
func classify(err error) error {
	var te *textproto.Error
	if errors.As(err, &te) && te.Code >= 500 {
		msg := strings.ToLower(te.Msg)
		if strings.Contains(msg, "try again") || strings.Contains(msg, "rate") || strings.Contains(msg, "limit") || strings.Contains(msg, "quota") {
			return err
		}
		return PermanentError{err}
	}
	return err
}

// Log is a placeholder channel (SMS, push) that only logs. It shows where a
// real provider plugs in: implement Channel and register it in cmd/delivery.
type Log struct {
	Kind string
	Out  *slog.Logger
}

func (l Log) Name() string { return l.Kind }

func (l Log) Send(_ context.Context, m Message) error {
	l.Out.Info("would send", "channel", l.Kind, "to", m.To, "text", m.SMS)
	return nil
}
