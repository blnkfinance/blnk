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
package model

import (
	"fmt"
	"sync"
	"testing"
)

// TestPrecisionCache_ConcurrentAccess exercises the read-through precision cache
// from many goroutines at once. Run under -race to assert there is no data race
// on the shared map.
func TestPrecisionCache_ConcurrentAccess(t *testing.T) {
	resetPrecisionCache()

	const goroutines = 50
	const perGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := fmt.Sprintf("txn_%d_%d", g, i%10)
				setCachedPrecision(id, float64(i))
				if v, ok := getCachedPrecision(id); ok && v < 0 {
					t.Errorf("unexpected negative precision %f", v)
				}
			}
		}(g)
	}
	wg.Wait()

	if v, ok := getCachedPrecision("txn_0_0"); !ok {
		t.Errorf("expected txn_0_0 to be cached, got %v", v)
	}
}

// TestPrecisionCache_SizeBoundResets verifies the cache resets once it reaches the
// configured bound, so it can never grow without limit.
func TestPrecisionCache_SizeBoundResets(t *testing.T) {
	resetPrecisionCache()

	// Fill to exactly the bound.
	for i := 0; i < precisionCacheMaxEntries; i++ {
		setCachedPrecision(fmt.Sprintf("txn_%d", i), float64(i))
	}

	precisionCacheMu.RLock()
	sizeAtBound := len(precisionCache)
	precisionCacheMu.RUnlock()
	if sizeAtBound != precisionCacheMaxEntries {
		t.Fatalf("expected cache size %d, got %d", precisionCacheMaxEntries, sizeAtBound)
	}

	// One more insert trips the bound and resets, so the map holds only the new entry.
	setCachedPrecision("txn_overflow", 1)

	precisionCacheMu.RLock()
	sizeAfter := len(precisionCache)
	_, hasOverflow := precisionCache["txn_overflow"]
	precisionCacheMu.RUnlock()

	if sizeAfter != 1 {
		t.Fatalf("expected cache to reset to 1 entry after bound, got %d", sizeAfter)
	}
	if !hasOverflow {
		t.Errorf("expected the overflow entry to be retained after reset")
	}
}

func resetPrecisionCache() {
	precisionCacheMu.Lock()
	defer precisionCacheMu.Unlock()
	precisionCache = make(map[string]float64)
}
