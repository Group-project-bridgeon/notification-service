package email

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/smtp"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// OutboundEmail represents a completely rendered email ready for transport dispatch.
type OutboundEmail struct {
	FromName    string
	FromAddress string
	To          []string
	Cc          []string
	ReplyTo     *string
	Subject     string
	HTMLBody    string
	TextBody    string
	Headers     map[string]string
}

// SendResult carries transport-level confirmation identifiers.
type SendResult struct {
	ProviderMessageID string
}

// EmailSender defines a provider-neutral interface for sending emails.
type EmailSender interface {
	Send(ctx context.Context, email *OutboundEmail) (*SendResult, error)
}

// LogSender is a development email sender that prints rendered emails to standard output or a designated writer.
// It is strictly forbidden in production.
type LogSender struct {
	env    string
	out    io.Writer
	logger *slog.Logger
}

// NewLogSender creates a LogSender instance. Returns an error if env is production.
func NewLogSender(env string, logger *slog.Logger, customOutput ...io.Writer) (*LogSender, error) {
	if strings.ToLower(env) == "production" {
		return nil, errors.New("LogSender is strictly prohibited in production environment")
	}
	var out io.Writer = os.Stdout
	if len(customOutput) > 0 && customOutput[0] != nil {
		out = customOutput[0]
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSender{
		env:    env,
		out:    out,
		logger: logger,
	}, nil
}

// Send logs the rendered email content to the configured writer.
func (l *LogSender) Send(_ context.Context, msg *OutboundEmail) (*SendResult, error) {
	if strings.ToLower(l.env) == "production" {
		return nil, errors.New("cannot use LogSender in production")
	}

	providerID := fmt.Sprintf("log-%s", uuid.New())

	fmt.Fprintf(l.out, "\n==================== EMAIL LOG SENDER ====================\n")
	fmt.Fprintf(l.out, "Provider Message ID: %s\n", providerID)
	fmt.Fprintf(l.out, "From: %s <%s>\n", msg.FromName, msg.FromAddress)
	fmt.Fprintf(l.out, "To:   %s\n", strings.Join(msg.To, ", "))
	if len(msg.Cc) > 0 {
		fmt.Fprintf(l.out, "Cc:   %s\n", strings.Join(msg.Cc, ", "))
	}
	if msg.ReplyTo != nil && *msg.ReplyTo != "" {
		fmt.Fprintf(l.out, "Reply-To: %s\n", *msg.ReplyTo)
	}
	fmt.Fprintf(l.out, "Subject: %s\n", msg.Subject)
	fmt.Fprintf(l.out, "--- Text Body ---\n%s\n", msg.TextBody)
	fmt.Fprintf(l.out, "--- HTML Body ---\n%s\n", msg.HTMLBody)
	fmt.Fprintf(l.out, "==========================================================\n\n")

	return &SendResult{ProviderMessageID: providerID}, nil
}

// SMTPOptions configures SMTP connection parameters.
type SMTPOptions struct {
	Host     string
	Port     int
	Username string
	Password string
	TLSMode  string // "none", "starttls", "ssl_tls", "tls"
	Timeout  time.Duration
}

// SMTPSender implements EmailSender using standard SMTP with TLS, authentication, and timeouts.
type SMTPSender struct {
	opts   SMTPOptions
	logger *slog.Logger
}

// NewSMTPSender creates an SMTPSender instance.
func NewSMTPSender(opts SMTPOptions, logger *slog.Logger) *SMTPSender {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.TLSMode == "" {
		opts.TLSMode = "starttls"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SMTPSender{
		opts:   opts,
		logger: logger,
	}
}

// Send delivers an email over SMTP.
func (s *SMTPSender) Send(ctx context.Context, msg *OutboundEmail) (*SendResult, error) {
	addr := fmt.Sprintf("%s:%d", s.opts.Host, s.opts.Port)
	tlsConfig := &tls.Config{
		ServerName: s.opts.Host,
		MinVersion: tls.VersionTLS12,
	}

	var conn net.Conn
	var err error
	dialer := &net.Dialer{Timeout: s.opts.Timeout}

	tlsMode := strings.ToLower(s.opts.TLSMode)
	if tlsMode == "ssl_tls" || tlsMode == "tls" {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed connecting to SMTP server at %s: %w", addr, err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, s.opts.Host)
	if err != nil {
		return nil, fmt.Errorf("failed creating SMTP client: %w", err)
	}
	defer client.Close()

	if tlsMode == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); ok {
			if err := client.StartTLS(tlsConfig); err != nil {
				return nil, fmt.Errorf("STARTTLS negotiation failed: %w", err)
			}
		} else {
			return nil, errors.New("SMTP server does not support STARTTLS")
		}
	}

	if s.opts.Username != "" {
		auth := smtp.PlainAuth("", s.opts.Username, s.opts.Password, s.opts.Host)
		if err := client.Auth(auth); err != nil {
			return nil, fmt.Errorf("SMTP authentication failed: %w", err)
		}
	}

	fromAddr := msg.FromAddress
	if fromAddr == "" {
		fromAddr = "no-reply@fashionerp.com"
	}
	if err := client.Mail(fromAddr); err != nil {
		return nil, fmt.Errorf("SMTP MAIL command failed: %w", err)
	}

	allRecipients := append(append([]string{}, msg.To...), msg.Cc...)
	for _, rcpt := range allRecipients {
		if err := client.Rcpt(rcpt); err != nil {
			return nil, fmt.Errorf("SMTP RCPT command failed for %s: %w", rcpt, err)
		}
	}

	w, err := client.Data()
	if err != nil {
		return nil, fmt.Errorf("SMTP DATA command failed: %w", err)
	}

	mimeMsg := buildMIMEMessage(msg)
	if _, err := w.Write([]byte(mimeMsg)); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed writing SMTP message body: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed closing SMTP DATA writer: %w", err)
	}

	_ = client.Quit()

	providerID := fmt.Sprintf("smtp-%s", uuid.New())
	return &SendResult{ProviderMessageID: providerID}, nil
}

func buildMIMEMessage(msg *OutboundEmail) string {
	var sb strings.Builder
	boundary := fmt.Sprintf("boundary_%s", uuid.New().String())

	fromHeader := msg.FromAddress
	if msg.FromName != "" {
		fromHeader = fmt.Sprintf("%s <%s>", msg.FromName, msg.FromAddress)
	}

	sb.WriteString(fmt.Sprintf("From: %s\r\n", fromHeader))
	sb.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(msg.To, ", ")))
	if len(msg.Cc) > 0 {
		sb.WriteString(fmt.Sprintf("Cc: %s\r\n", strings.Join(msg.Cc, ", ")))
	}
	if msg.ReplyTo != nil && *msg.ReplyTo != "" {
		sb.WriteString(fmt.Sprintf("Reply-To: %s\r\n", *msg.ReplyTo))
	}
	sb.WriteString(fmt.Sprintf("Subject: %s\r\n", msg.Subject))
	sb.WriteString("MIME-Version: 1.0\r\n")
	sb.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=\"%s\"\r\n", boundary))
	sb.WriteString("\r\n")

	// Plain text part
	sb.WriteString(fmt.Sprintf("--%s\r\n", boundary))
	sb.WriteString("Content-Type: text/plain; charset=\"UTF-8\"\r\n")
	sb.WriteString("Content-Transfer-Encoding: 7bit\r\n\r\n")
	sb.WriteString(msg.TextBody)
	sb.WriteString("\r\n\r\n")

	// HTML part
	sb.WriteString(fmt.Sprintf("--%s\r\n", boundary))
	sb.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	sb.WriteString("Content-Transfer-Encoding: 7bit\r\n\r\n")
	sb.WriteString(msg.HTMLBody)
	sb.WriteString("\r\n\r\n")

	sb.WriteString(fmt.Sprintf("--%s--\r\n", boundary))

	return sb.String()
}
