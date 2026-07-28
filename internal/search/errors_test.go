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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsMemoryBackpressure(t *testing.T) {
	t.Run("recognizes wrapped native response", func(t *testing.T) {
		err := fmt.Errorf(
			"failed to upsert document in Typesense: %w",
			errors.New(`status: 422 response: {"message":"Rejecting write: running out of resource type: OUT_OF_MEMORY"}`),
		)
		assert.True(t, IsMemoryBackpressure(err))
		marked := MarkMemoryBackpressure(err)
		assert.ErrorIs(t, marked, ErrMemoryBackpressure)
		assert.True(t, IsMemoryBackpressure(marked))
	})

	t.Run("is case insensitive", func(t *testing.T) {
		assert.True(t, IsMemoryBackpressure(errors.New("resource type: out_of_memory")))
	})

	t.Run("does not classify other failures", func(t *testing.T) {
		assert.False(t, IsMemoryBackpressure(errors.New("connection refused")))
		assert.False(t, IsMemoryBackpressure(errors.New("OUT_OF_DISK")))
		assert.False(t, IsMemoryBackpressure(nil))
	})

	t.Run("does not mark unrelated errors", func(t *testing.T) {
		err := errors.New("connection refused")
		assert.Same(t, err, MarkMemoryBackpressure(err))
	})
}
