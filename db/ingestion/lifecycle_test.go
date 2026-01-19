// Package ingestion - Lifecycle state machine tests
package ingestion

import (
	"context"
	"testing"
	"time"
)

func TestLifecyclePhaseProgression(t *testing.T) {
	// Test that phases progress correctly
	phases := []IngestionPhase{
		PhaseInit,
		PhaseFetching,
		PhaseNormalizing,
		PhaseValidating,
		PhaseStaging,
		PhaseBackedUp,
		PhaseCommitting,
		PhaseActive,
	}

	for i, phase := range phases {
		if int(phase) != i {
			t.Errorf("phase %s has wrong value: expected %d, got %d", phase, i, int(phase))
		}
	}
}

func TestLifecyclePhaseString(t *testing.T) {
	tests := []struct {
		phase    IngestionPhase
		expected string
	}{
		{PhaseInit, "init"},
		{PhaseFetching, "fetching"},
		{PhaseNormalizing, "normalizing"},
		{PhaseValidating, "validating"},
		{PhaseStaging, "staging"},
		{PhaseBackedUp, "backed_up"},
		{PhaseCommitting, "committing"},
		{PhaseActive, "active"},
		{PhaseFailed, "failed"},
	}

	for _, tt := range tests {
		if got := tt.phase.String(); got != tt.expected {
			t.Errorf("phase.String() = %s, want %s", got, tt.expected)
		}
	}
}

func TestProductionGuards(t *testing.T) {
	// Test that production guards block mock pricing
	config := &LifecycleConfig{
		Environment:      "production",
		AllowMockPricing: true,
	}

	lifecycle := &Lifecycle{
		config: config,
		state:  &LifecycleState{},
	}

	err := lifecycle.enforceProductionGuards()
	if err == nil {
		t.Error("expected error for mock pricing in production, got nil")
	}

	// Should pass in development
	config.Environment = "development"
	err = lifecycle.enforceProductionGuards()
	if err != nil {
		t.Errorf("unexpected error in development: %v", err)
	}
}

func TestDefaultLifecycleConfig(t *testing.T) {
	config := DefaultLifecycleConfig()

	if config.Environment != "production" {
		t.Errorf("default environment should be production, got %s", config.Environment)
	}

	if config.AllowMockPricing {
		t.Error("AllowMockPricing should be false by default")
	}

	if config.MinCoverage != 95.0 {
		t.Errorf("MinCoverage should be 95.0, got %f", config.MinCoverage)
	}

	if config.Timeout != 30*time.Minute {
		t.Errorf("Timeout should be 30 minutes, got %v", config.Timeout)
	}
}

func TestIngestionStateInitialization(t *testing.T) {
	state := &LifecycleState{
		Phase: PhaseInit,
	}

	if state.Phase != PhaseInit {
		t.Error("initial phase should be PhaseInit")
	}

	if state.SnapshotID != nil {
		t.Error("SnapshotID should be nil before commit")
	}

	if state.BackupVerified {
		t.Error("BackupVerified should be false initially")
	}
}

func TestLifecycleFailure(t *testing.T) {
	lifecycle := &Lifecycle{
		state: &LifecycleState{
			Phase:     PhaseValidating,
			StartTime: time.Now(),
		},
	}

	result, _ := lifecycle.fail(context.Canceled)

	if result.Success {
		t.Error("failed lifecycle should have Success=false")
	}

	if lifecycle.state.Phase != PhaseFailed {
		t.Error("failed lifecycle should have PhaseFailed")
	}

	if len(lifecycle.state.Errors) == 0 {
		t.Error("failed lifecycle should have errors recorded")
	}
}

func TestSnapshotStates(t *testing.T) {
	states := []SnapshotState{
		StatePending,
		StateStaging,
		StateReady,
		StateFailed,
		StateArchived,
	}

	expected := []string{"pending", "staging", "ready", "failed", "archived"}

	for i, state := range states {
		if string(state) != expected[i] {
			t.Errorf("state %v should be %s, got %s", state, expected[i], string(state))
		}
	}
}
