// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package version provides helpers for parsing and comparing Kubernetes
// version strings.
package version

import (
	"strings"

	"github.com/Masterminds/semver/v3"
)

// constraintGreaterEqual134 matches Kubernetes versions 1.34.0 and higher.
var constraintGreaterEqual134 = mustNewConstraint(">= 1.34-0")

func mustNewConstraint(constraint string) *semver.Constraints {
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		panic(err)
	}

	return c
}

// normalize returns the normalized version string by removing the leading 'v'
// and any suffixes like '-rc1', '+build', etc.
func normalize(version string) string {
	v := strings.ReplaceAll(version, "v", "")
	if idx := strings.IndexAny(v, "-+"); idx != -1 {
		v = v[:idx]
	}

	return v
}

// IsKubernetesVersionGreaterEqual134 reports whether the given Kubernetes version string is
// 1.34.0 or newer.
func IsKubernetesVersionGreaterEqual134(version string) bool {
	v, err := semver.NewVersion(normalize(version))
	if err != nil {
		return false
	}

	return constraintGreaterEqual134.Check(v)
}
