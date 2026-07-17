package callback

import (
	"context"
	"encoding/json"
	"log"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Invoker runs batch callbacks (Ruby classes in legacy mode; log-only default).
type Invoker interface {
	Invoke(ctx context.Context, cb protocol.CallbackMessage) error
}

// DLTProducer publishes failed callback payloads.
type DLTProducer interface {
	ProduceDLT(ctx context.Context, key string, payload []byte) error
}

// LogInvoker records callback class names (Phase 3 default).
type LogInvoker struct{}

func (LogInvoker) Invoke(_ context.Context, cb protocol.CallbackMessage) error {
	log.Printf("[kbatch-daemon] callback batch_id=%s outcome=%s on_success=%s on_complete=%s",
		cb.BatchID, cb.Outcome, cb.OnSuccess, cb.OnComplete)
	return nil
}

// Processor claims and invokes batch callbacks.
type Processor struct {
	Store   *store.RedisStore
	Invoker Invoker
	DLT     DLTProducer
	NodeID  string
}

type Outcome struct {
	CommitOffset bool
}

func (p *Processor) Process(ctx context.Context, raw []byte) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var cb protocol.CallbackMessage
	if err := json.Unmarshal(raw, &cb); err != nil {
		log.Printf("[kbatch-daemon] malformed callback JSON: %v", err)
		instrument.CallbackFailed("", "", "", "json.SyntaxError", err.Error())
		if p.DLT != nil {
			dlt := map[string]interface{}{
				"dlt_type":          "malformed_callback",
				"dlt_raw_payload":   string(raw),
				"dlt_error_class":   "json.SyntaxError",
				"dlt_error_message": err.Error(),
			}
			rawDLT, _ := json.Marshal(dlt)
			_ = p.DLT.ProduceDLT(ctx, "malformed_callback", rawDLT)
			instrument.DLTPublished("", "", "malformed_callback", "callbacks")
		}
		return out, nil
	}
	if cb.BatchID == "" {
		return out, nil
	}
	if !cb.Preclaimed {
		kind := claimKind(cb.Outcome)
		won, err := p.Store.ClaimCallback(ctx, cb.BatchID, p.NodeID, kind)
		if err != nil {
			out.CommitOffset = false
			return out, err
		}
		if !won {
			return out, nil
		}
	}
	// Events Lua preclaims claim stamps without callback_dispatched_by;
	// record the runner so the UI "Callback ran on" field is populated.
	if err := p.Store.RecordCallbackRunner(ctx, cb.BatchID, p.NodeID); err != nil {
		log.Printf("[kbatch-daemon] record callback runner batch_id=%s: %v", cb.BatchID, err)
	}
	if p.Invoker == nil {
		return out, nil
	}
	if err := p.Invoker.Invoke(ctx, cb); err != nil {
		log.Printf("[kbatch-daemon] callback invoke batch_id=%s: %v", cb.BatchID, err)
		method := callbackMethod(cb)
		class := callbackClass(cb, method)
		instrument.CallbackFailed(cb.BatchID, class, method, err.Error(), err.Error())
		if p.DLT != nil {
			dlt := map[string]interface{}{
				"batch_id":          cb.BatchID,
				"dlt_type":          "callback_error",
				"dlt_error_message": err.Error(),
				"on_success":        cb.OnSuccess,
				"on_complete":       cb.OnComplete,
				"outcome":           cb.Outcome,
			}
			rawDLT, _ := json.Marshal(dlt)
			_ = p.DLT.ProduceDLT(ctx, cb.BatchID, rawDLT)
			instrument.DLTPublished("", cb.BatchID, "callback_error", "callbacks")
		}
		return out, nil
	}
	method := callbackMethod(cb)
	class := callbackClass(cb, method)
	instrument.CallbackInvoked(cb.BatchID, class, method)
	return out, nil
}

func claimKind(outcome string) string {
	switch outcome {
	case "success", "success_only":
		return "success"
	default:
		return "complete"
	}
}

func callbackMethod(cb protocol.CallbackMessage) string {
	if (cb.Outcome == "success" || cb.Outcome == "success_only") && cb.OnSuccess != "" {
		return "on_success"
	}
	return "on_complete"
}

func callbackClass(cb protocol.CallbackMessage, method string) string {
	if method == "on_success" {
		return cb.OnSuccess
	}
	return cb.OnComplete
}
