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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

// ErrQueueBackpressure is returned when enqueue is rejected to keep Redis below OOM.
var ErrQueueBackpressure = errors.New("queue backpressure: redis memory or pending task limit reached")

func (q *Queue) assertEnqueueAllowed(ctx context.Context) error {
	if q == nil || q.config == nil || !q.config.Queue.EnableBackpressure {
		return nil
	}

	reject, err := q.shouldRejectEnqueue(ctx)
	if err != nil {
		logrus.WithError(err).Warn("queue backpressure check failed; allowing enqueue")
		return nil
	}
	if !reject {
		return nil
	}

	metrics.QueueBackpressureRejectedTotal.Add(ctx, 1,
		otelmetric.WithAttributes(attribute.String("reason", "limit")),
	)
	return ErrQueueBackpressure
}

func (q *Queue) shouldRejectEnqueue(ctx context.Context) (bool, error) {
	interval := q.config.Queue.BackpressureCheckInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	q.bpMu.Lock()
	if !q.bpCheckedAt.IsZero() && time.Since(q.bpCheckedAt) < interval {
		reject := q.bpReject
		q.bpMu.Unlock()
		return reject, nil
	}
	q.bpMu.Unlock()

	reject, err := q.evaluateBackpressure(ctx)
	if err != nil {
		return false, err
	}

	q.bpMu.Lock()
	q.bpCheckedAt = time.Now()
	q.bpReject = reject
	q.bpMu.Unlock()

	return reject, nil
}

func (q *Queue) evaluateBackpressure(ctx context.Context) (bool, error) {
	if q.redis != nil {
		snap, err := redisMemorySnapshot(ctx, q.redis)
		if err != nil {
			return false, err
		}
		if ratio, ok := snap.usageRatio(); ok {
			threshold := q.config.Queue.BackpressureMemoryPercent
			if threshold <= 0 {
				threshold = 85
			}
			if ratio*100 >= threshold {
				return true, nil
			}
		}
	}

	maxPending := q.config.Queue.BackpressureMaxPendingTasks
	if maxPending > 0 && q.Inspector != nil {
		pending, err := q.transactionQueuePendingCount()
		if err != nil {
			return false, err
		}
		if pending >= maxPending {
			return true, nil
		}
	}

	return false, nil
}

func (q *Queue) transactionQueuePendingCount() (int, error) {
	total := 0
	for i := 1; i <= q.config.Queue.NumberOfQueues; i++ {
		queueName := fmt.Sprintf("%s_%d", q.config.Queue.TransactionQueue, i)
		n, err := queuePendingTotal(q.Inspector, queueName)
		if err != nil {
			return 0, err
		}
		total += n
	}
	if q.config.Queue.EnableHotLane {
		n, err := queuePendingTotal(q.Inspector, q.config.Queue.HotQueueName)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func queuePendingTotal(inspector *asynq.Inspector, queueName string) (int, error) {
	info, err := inspector.GetQueueInfo(queueName)
	if err != nil {
		if errors.Is(err, asynq.ErrQueueNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return info.Pending + info.Scheduled + info.Retry, nil
}

type redisMemoryInfo struct {
	usedBytes int64
	maxBytes  int64
}

func (m redisMemoryInfo) usageRatio() (float64, bool) {
	if m.maxBytes <= 0 {
		return 0, false
	}
	return float64(m.usedBytes) / float64(m.maxBytes), true
}

func redisMemorySnapshot(ctx context.Context, rdb redis.UniversalClient) (redisMemoryInfo, error) {
	raw, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return redisMemoryInfo{}, err
	}
	return parseRedisMemoryInfo(raw), nil
}

func parseRedisMemoryInfo(info string) redisMemoryInfo {
	var out redisMemoryInfo
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "used_memory":
			out.usedBytes = parseRedisInfoInt64(val)
		case "maxmemory":
			out.maxBytes = parseRedisInfoInt64(val)
		}
	}
	return out
}

func parseRedisInfoInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
