// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Heartbeat tracks periodic component activity and exposes a controller-runtime health checker.
type Heartbeat struct {
	activityTimeout time.Duration
	checkTimeout    bool
	lastActivity    time.Time
	mutex           sync.Mutex
}

// NewHeartbeat creates a heartbeat checker with the given inactivity timeout.
func NewHeartbeat(activityTimeout time.Duration) *Heartbeat {
	return &Heartbeat{
		activityTimeout: activityTimeout,
		lastActivity:    time.Now(),
	}
}

// StartMonitoring enables timeout checks and resets last activity to now.
func (h *Heartbeat) StartMonitoring() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.checkTimeout = true
	h.lastActivity = time.Now()
}

// UpdateLastActivity marks component activity at the current time.
func (h *Heartbeat) UpdateLastActivity() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.lastActivity = time.Now()
}

// Check implements controller-runtime healthz checker interface.
func (h *Heartbeat) Check(_ *http.Request) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.checkTimeout {
		return nil
	}

	now := time.Now()
	ago := now.Sub(h.lastActivity)
	if now.After(h.lastActivity.Add(h.activityTimeout)) {
		return fmt.Errorf("last activity more than %s ago", ago)
	}

	return nil
}
