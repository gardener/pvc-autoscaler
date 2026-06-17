// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// Heartbeat tracks periodic component activity and exposes a controller-runtime health checker.
type Heartbeat struct {
	activityTimeout time.Duration
	checkTimeout    bool
	lastActivity    time.Time
	clock           clock.Clock
	mutex           sync.Mutex
}

// NewHeartbeat creates a heartbeat checker with the given inactivity timeout.
func NewHeartbeat(activityTimeout time.Duration, clk clock.Clock) *Heartbeat {
	return &Heartbeat{
		activityTimeout: activityTimeout,
		lastActivity:    clk.Now(),
		clock:           clk,
	}
}

// StartMonitoring enables timeout checks and resets last activity to now.
func (h *Heartbeat) StartMonitoring() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.checkTimeout = true
	h.lastActivity = h.clock.Now()
}

// UpdateLastActivity marks component activity at the current time.
func (h *Heartbeat) UpdateLastActivity() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.lastActivity = h.clock.Now()
}

// Check implements controller-runtime healthz checker interface.
func (h *Heartbeat) Check(_ *http.Request) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.checkTimeout {
		return nil
	}

	now := h.clock.Now()
	ago := now.Sub(h.lastActivity)
	if now.After(h.lastActivity.Add(h.activityTimeout)) {
		return fmt.Errorf("last activity more than %s ago", ago)
	}

	return nil
}
