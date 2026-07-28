/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package search

import (
	"errors"
	"fmt"
	"strings"
)

const typesenseOutOfMemoryCode = "OUT_OF_MEMORY"

// ErrMemoryBackpressure marks an indexing task that Typesense rejected at its
// native memory limit. Workers use this sentinel to retry without consuming
// their normal failure budget.
var ErrMemoryBackpressure = errors.New("Typesense memory backpressure")

// IsMemoryBackpressure reports whether Typesense rejected a write because its
// native memory-used-max-percentage limit was reached.
func IsMemoryBackpressure(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrMemoryBackpressure) ||
		strings.Contains(strings.ToUpper(err.Error()), typesenseOutOfMemoryCode)
}

// MarkMemoryBackpressure wraps a native Typesense rejection with the sentinel
// used by the index worker's retry policy.
func MarkMemoryBackpressure(err error) error {
	if !IsMemoryBackpressure(err) || errors.Is(err, ErrMemoryBackpressure) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrMemoryBackpressure, err)
}
