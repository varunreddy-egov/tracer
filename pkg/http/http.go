// tracer/http/http.go
package http

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/varunreddy-egov/tracer/pkg/logger"
)

// Type aliases for standard HTTP types
type (
	ResponseWriter = http.ResponseWriter
	Request        = http.Request
	Handler        = http.Handler
	HandlerFunc    = http.HandlerFunc
	ServeMux       = http.ServeMux
)

// Server functions
func NewServeMux() *ServeMux {
	return http.NewServeMux()
}

func ListenAndServe(addr string, handler Handler) error {
	return http.ListenAndServe(addr, handler)
}

// Middleware
func LoggingMiddleware(next Handler) Handler {
	return HandlerFunc(func(w ResponseWriter, r *Request) {
		start := time.Now()
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		logger.Log.Info("Incoming Request",
			"method", r.Method,
			"uri", r.RequestURI,
			"query", r.URL.RawQuery,
			"body", string(bodyBytes))

		// Capture response
		rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		logger.Log.Info("Response",
			"status", rec.statusCode,
			"duration", duration.String())
	})
}

type responseRecorder struct {
	ResponseWriter
	statusCode int
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
	rr.ResponseWriter.WriteHeader(code)
}

// Client
type LoggingRoundTripper struct {
	rt http.RoundTripper
}

func (lrt *LoggingRoundTripper) RoundTrip(req *Request) (*http.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	logger.Log.Info("Outgoing Request",
		"method", req.Method,
		"url", req.URL.String(),
		"body", string(bodyBytes))

	resp, err := lrt.rt.RoundTrip(req)
	if err != nil {
		logger.Log.Error("Request Error", "error", err)
		return nil, err
	}

	respBody, _ := io.ReadAll(resp.Body)
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))

	logger.Log.Info("Response",
		"status", resp.StatusCode,
		"body", string(respBody))
	return resp, nil
}

func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: &LoggingRoundTripper{rt: http.DefaultTransport},
	}
}
