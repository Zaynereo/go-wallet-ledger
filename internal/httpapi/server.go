// Package httpapi is the versioned JSON API for the ledger.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/domain"
	"github.com/Zaynereo/go-wallet-ledger/internal/ledger"
)

type contextKey struct{}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// Handler serves /v1 routes.
type Handler struct {
	svc    *ledger.Service
	ready  func(context.Context) error
	logger *slog.Logger
}

func New(svc *ledger.Service, ready func(context.Context) error, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{svc: svc, ready: ready, logger: logger}
}

// Routes returns the HTTP handler with logging, request ids, and panic recovery.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", h.health)
	mux.HandleFunc("GET /v1/ready", h.readyCheck)
	mux.HandleFunc("POST /v1/accounts", h.createAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", h.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", h.listEntries)
	mux.HandleFunc("POST /v1/transfers", h.transfer)
	mux.HandleFunc("POST /v1/deposits", h.deposit)
	mux.HandleFunc("POST /v1/withdrawals", h.withdrawal)
	mux.HandleFunc("GET /v1/transactions/{id}", h.getTransaction)
	mux.HandleFunc("GET /v1/reconciliation", h.reconcile)
	mux.HandleFunc("/", h.notFound)
	return h.recover(h.requestIDs(h.accessLog(mux)))
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) readyCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if h.ready == nil || h.ready(ctx) != nil {
		h.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) notFound(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusNotFound, errorEnvelope("not_found", "resource not found", nil))
}

type errorEnvelopeBody struct {
	Error struct {
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Fields  map[string]string `json:"fields,omitempty"`
	} `json:"error"`
}

func errorEnvelope(code, message string, fields map[string]string) errorEnvelopeBody {
	var body errorEnvelopeBody
	body.Error.Code = code
	body.Error.Message = message
	body.Error.Fields = fields
	return body
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		h.logger.Error("write json", "err", err)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var de *domain.Error
	if errors.As(err, &de) {
		status := de.Status
		if status == 0 {
			status = http.StatusBadRequest
		}
		h.writeJSON(w, status, errorEnvelope(de.Code, de.Message, de.Fields))
		return
	}
	h.logger.Error("request failed", "err", err, "request_id", RequestID(r.Context()))
	h.writeJSON(w, http.StatusInternalServerError, errorEnvelope("internal_error", "internal error", nil))
}

func (h *Handler) requestIDs(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if !requestIDPattern.MatchString(rid) {
			rid = newRequestID()
		}
		w.Header().Set("X-Request-Id", rid)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, rid)))
	})
}

func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(contextKey{}).(string)
	return v
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "rid"
	}
	return hex.EncodeToString(b[:])
}

func (h *Handler) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		h.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", RequestID(r.Context()),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.status = http.StatusOK
		w.wrote = true
	}
	return w.ResponseWriter.Write(b)
}

func (h *Handler) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("panic", "err", rec, "stack", string(debug.Stack()), "request_id", RequestID(r.Context()))
				h.writeJSON(w, http.StatusInternalServerError, errorEnvelope("internal_error", "internal error", nil))
			}
		}()
		next.ServeHTTP(w, r)
	})
}
