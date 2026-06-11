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

package blnk

import (
	"context"
	"testing"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateBulkTransactions_SemaphoreSaturated asserts that when the async-bulk
// semaphore is fully held, an async request fails fast with a typed rate-limited
// error and a nil result, so the API layer reports 429 instead of dereferencing
// the nil result and panicking.
func TestCreateBulkTransactions_SemaphoreSaturated(t *testing.T) {
	// Hold every slot so the next TryAcquire fails.
	require.NoError(t, asyncBulkSemaphore.Acquire(context.Background(), 100))
	defer asyncBulkSemaphore.Release(100)

	l := &Blnk{}
	result, err := l.CreateBulkTransactions(context.Background(), &model.BulkTransactionRequest{
		RunAsync:     true,
		Transactions: []*model.Transaction{{Reference: "ref_saturated"}},
	})

	require.Error(t, err)
	assert.Nil(t, result, "a saturated semaphore must return a nil result")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrRateLimited, apiErr.Code)
}
