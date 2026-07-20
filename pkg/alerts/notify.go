package alerts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

type Multi struct {
	cfg Config
	hc  *http.Client
}

func NewMulti(cfg Config) *Multi {
	return &Multi{
		cfg: cfg,
		hc:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (m *Multi) Deliver(p Payload) {
	p = p.withDefaults()
	if m.cfg.ChannelSlack && m.cfg.SlackWebhookURL != "" && m.cfg.EncryptionSalt != "" {
		if err := m.deliverSlack(p); err != nil {
			log.Printf("[kbatch-alerts] slack: %v", err)
		}
	}
	if m.cfg.ChannelWebhook && len(m.cfg.WebhookURLs) > 0 && m.cfg.EncryptionSalt != "" {
		if err := m.deliverWebhooks(p); err != nil {
			log.Printf("[kbatch-alerts] webhook: %v", err)
		}
	}
	if m.cfg.ChannelEmail && m.cfg.EmailTo != "" && m.cfg.EmailSMTPAddress != "" {
		if err := m.deliverEmail(p); err != nil {
			log.Printf("[kbatch-alerts] email: %v", err)
		}
	}
	if m.cfg.ChannelMetrics && m.cfg.MetricsEnabled {
		m.deliverMetrics(p)
	}
}

func (m *Multi) deliverSlack(p Payload) error {
	header := p.Event + ": " + p.Title
	if len(header) > 150 {
		header = header[:150]
	}
	body := map[string]interface{}{
		"text": fmt.Sprintf("[%s] %s", p.Severity, p.Title),
		"blocks": []map[string]interface{}{
			{"type": "header", "text": map[string]string{"type": "plain_text", "text": strings.ToUpper(header)}},
			{"type": "section", "text": map[string]string{"type": "mrkdwn", "text": truncate(p.Summary, 2900)}},
			{"type": "context", "elements": []map[string]string{
				{"type": "mrkdwn", "text": fmt.Sprintf("rule=`%s` · `%s` · source=go", p.RuleID, p.Fingerprint)},
			}},
		},
	}
	if p.Link != "" {
		blocks := body["blocks"].([]map[string]interface{})
		blocks = append(blocks, map[string]interface{}{
			"type": "actions",
			"elements": []map[string]interface{}{
				{
					"type": "button",
					"text": map[string]string{"type": "plain_text", "text": "Open dashboard"},
					"url":  p.Link,
				},
			},
		})
		body["blocks"] = blocks
	}
	return m.postJSON(m.cfg.SlackWebhookURL, body)
}

func (m *Multi) deliverWebhooks(p Payload) error {
	var first error
	for _, u := range m.cfg.WebhookURLs {
		if err := m.postJSON(u, p); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (m *Multi) deliverEmail(p Payload) error {
	from := m.cfg.EmailFrom
	if from == "" {
		from = "kafka-batch-alerts@localhost"
	}
	addr := fmt.Sprintf("%s:%d", m.cfg.EmailSMTPAddress, m.cfg.EmailSMTPPort)
	subject := fmt.Sprintf("[%s] %s: %s", p.Severity, p.Event, p.Title)
	msg := []byte(fmt.Sprintf("To: %s\r\nFrom: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n\r\nrule=%s fingerprint=%s\r\n",
		m.cfg.EmailTo, from, subject, p.Summary, p.RuleID, p.Fingerprint))
	var auth smtp.Auth
	if m.cfg.EmailSMTPUser != "" {
		auth = smtp.PlainAuth("", m.cfg.EmailSMTPUser, m.cfg.EmailSMTPPassword, m.cfg.EmailSMTPAddress)
	}
	return smtp.SendMail(addr, auth, from, []string{m.cfg.EmailTo}, msg)
}

func (m *Multi) deliverMetrics(p Payload) {
	event := "alert.fired"
	if p.Event == "resolved" {
		event = "alert.resolved"
	}
	instrument.Emit(event, map[string]interface{}{
		"rule_id":     p.RuleID,
		"severity":   p.Severity,
		"fingerprint": p.Fingerprint,
		"event":       p.Event,
		"title":       p.Title,
	}, 0)
}

func (m *Multi) postJSON(url string, body interface{}) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := m.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("http %d", res.StatusCode)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
