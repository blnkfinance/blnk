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

package hooks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateHook_RetryCountBounds(t *testing.T) {
	t.Run("caps an excessive retry count", func(t *testing.T) {
		h := &Hook{URL: "https://example.com", Type: PreTransaction, RetryCount: 1_000_000_000}
		require.NoError(t, validateHook(h))
		assert.Equal(t, maxHookRetryCount, h.RetryCount, "retry count must be capped")
	})

	t.Run("defaults a negative retry count", func(t *testing.T) {
		h := &Hook{URL: "https://example.com", Type: PostTransaction, RetryCount: -1}
		require.NoError(t, validateHook(h))
		assert.Equal(t, 3, h.RetryCount)
	})

	t.Run("leaves an in-range retry count untouched", func(t *testing.T) {
		h := &Hook{URL: "https://example.com", Type: PreTransaction, RetryCount: 5}
		require.NoError(t, validateHook(h))
		assert.Equal(t, 5, h.RetryCount)
	})
}
