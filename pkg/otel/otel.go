package otel

import (
	"context"
	"fmt"
	"log"

	"digit-core/pkg/tracer/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Manager manages OpenTelemetry lifecycle
type Manager struct {
	config   *Config
	provider *sdktrace.TracerProvider
	tracer   *Tracer
}

// NewManager creates a new OpenTelemetry manager
func NewManager(cfg *Config) *Manager {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	
	return &Manager{
		config: cfg,
	}
}

// NewManagerFromEnv creates a new OpenTelemetry manager from environment variables
func NewManagerFromEnv() *Manager {
	cfg := ConfigFromEnv()
	return NewManager(cfg)
}

// Initialize initializes OpenTelemetry with the configured settings
func (m *Manager) Initialize() error {
	if !m.config.Enabled {
		log.Println("OpenTelemetry is disabled")
		return nil
	}

	provider, err := InitTracer(m.config)
	if err != nil {
		return fmt.Errorf("failed to initialize tracer: %w", err)
	}

	m.provider = provider
	m.tracer = NewTracer(m.config.ServiceName)

	log.Printf("OpenTelemetry initialized for service: %s", m.config.ServiceName)
	return nil
}

// GetTracer returns the tracer instance
func (m *Manager) GetTracer() *Tracer {
	if m.tracer == nil {
		// Return a tracer even if not initialized (will be no-op)
		return NewTracer(m.config.ServiceName)
	}
	return m.tracer
}

// GetTracerProvider returns the tracer provider
func (m *Manager) GetTracerProvider() *sdktrace.TracerProvider {
	return m.provider
}

// Shutdown gracefully shuts down OpenTelemetry
func (m *Manager) Shutdown(ctx context.Context) error {
	if m.provider != nil {
		return m.provider.Shutdown(ctx)
	}
	return nil
}

// CreateTracedPublisher creates a traced publisher with the manager's tracer
func (m *Manager) CreateTracedPublisher(cfg config.PubSubConfig) (*TracedPublisher, error) {
	return NewTracedPublisher(cfg, m.GetTracer())
}

// Global manager instance for easy access
var globalManager *Manager

// InitGlobal initializes the global OpenTelemetry manager
func InitGlobal(cfg *Config) error {
	globalManager = NewManager(cfg)
	return globalManager.Initialize()
}

// InitGlobalFromEnv initializes the global OpenTelemetry manager from environment variables
func InitGlobalFromEnv() error {
	globalManager = NewManagerFromEnv()
	return globalManager.Initialize()
}

// GetGlobalTracer returns the global tracer instance
func GetGlobalTracer() *Tracer {
	if globalManager == nil {
		// Return a default tracer if not initialized
		return NewTracer("unknown-service")
	}
	return globalManager.GetTracer()
}

// GetGlobalManager returns the global manager instance
func GetGlobalManager() *Manager {
	return globalManager
}

// ShutdownGlobal shuts down the global OpenTelemetry manager
func ShutdownGlobal(ctx context.Context) error {
	if globalManager != nil {
		return globalManager.Shutdown(ctx)
	}
	return nil
}
