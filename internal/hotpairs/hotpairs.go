package hotpairs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	LaneNormal = "normal"
	LaneHot    = "hot"
)

type State string

const (
	StateNormal      State = "normal"
	StatePromoting   State = "promoting"
	StateHot         State = "hot"
	StateCoolingDown State = "cooling_down"
)

type Config struct {
	Enabled                 bool
	HotQueueName            string
	HotPairTTL              time.Duration
	LockContentionThreshold int
}

type Manager struct {
	client    redis.UniversalClient
	enabled   bool
	hotQueue  string
	pairTTL   time.Duration
	threshold int
}

func NewManager(client redis.UniversalClient, cfg Config) *Manager {
	ttl := cfg.HotPairTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	threshold := cfg.LockContentionThreshold
	if threshold <= 0 {
		threshold = 3
	}

	hotQueue := cfg.HotQueueName
	if hotQueue == "" {
		hotQueue = "hot_transactions"
	}

	return &Manager{
		client:    client,
		enabled:   cfg.Enabled && client != nil,
		hotQueue:  hotQueue,
		pairTTL:   ttl,
		threshold: threshold,
	}
}

func (m *Manager) Enabled() bool {
	return m != nil && m.enabled
}

func (m *Manager) HotQueueName() string {
	if m == nil || m.hotQueue == "" {
		return "hot_transactions"
	}
	return m.hotQueue
}

func (m *Manager) PairKey(source, destination, currency string) string {
	return fmt.Sprintf("%s|%s|%s", source, destination, strings.ToUpper(currency))
}

func (m *Manager) GetState(ctx context.Context, pairKey string) (State, error) {
	if !m.Enabled() {
		return StateNormal, nil
	}

	state, err := m.client.Get(ctx, m.stateKey(pairKey)).Result()
	if err == redis.Nil {
		return StateNormal, nil
	}
	if err != nil {
		return StateNormal, err
	}

	switch State(state) {
	case StatePromoting, StateHot, StateCoolingDown:
		return State(state), nil
	default:
		return StateNormal, nil
	}
}

func (m *Manager) RecordContention(ctx context.Context, pairKey string) (int64, bool, error) {
	if !m.Enabled() {
		return 0, false, nil
	}

	pipe := m.client.TxPipeline()
	count := pipe.Incr(ctx, m.contentionKey(pairKey))
	pipe.Expire(ctx, m.contentionKey(pairKey), m.pairTTL)
	pipe.Set(ctx, m.activityKey(pairKey), "1", m.pairTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, false, err
	}

	current := count.Val()
	if current < int64(m.threshold) {
		return current, false, nil
	}

	state, err := m.GetState(ctx, pairKey)
	if err != nil {
		return current, false, err
	}
	if state == StateHot || state == StateCoolingDown || state == StatePromoting {
		_ = m.client.Expire(ctx, m.stateKey(pairKey), m.stateTTL()).Err()
		return current, state == StatePromoting, nil
	}

	if err := m.setState(ctx, pairKey, StatePromoting); err != nil {
		return current, false, err
	}

	return current, true, nil
}

func (m *Manager) HasRecentContention(ctx context.Context, pairKey string) (bool, error) {
	if !m.Enabled() {
		return false, nil
	}
	n, err := m.client.Exists(ctx, m.activityKey(pairKey)).Result()
	return n > 0, err
}

func (m *Manager) ActivateHot(ctx context.Context, pairKey string) error {
	if !m.Enabled() {
		return nil
	}

	pipe := m.client.TxPipeline()
	pipe.Set(ctx, m.stateKey(pairKey), string(StateHot), m.stateTTL())
	pipe.Set(ctx, m.activityKey(pairKey), "1", m.pairTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (m *Manager) StartCoolingDown(ctx context.Context, pairKey string) error {
	if !m.Enabled() {
		return nil
	}
	return m.setState(ctx, pairKey, StateCoolingDown)
}

func (m *Manager) SetNormal(ctx context.Context, pairKey string) error {
	if !m.Enabled() {
		return nil
	}

	pipe := m.client.TxPipeline()
	pipe.Del(ctx, m.stateKey(pairKey))
	pipe.Del(ctx, m.activityKey(pairKey))
	pipe.Del(ctx, m.contentionKey(pairKey))
	_, err := pipe.Exec(ctx)
	return err
}

func (m *Manager) setState(ctx context.Context, pairKey string, state State) error {
	return m.client.Set(ctx, m.stateKey(pairKey), string(state), m.stateTTL()).Err()
}

func (m *Manager) stateKey(pairKey string) string {
	return fmt.Sprintf("blnk:hot_pairs:state:%s", pairKey)
}

func (m *Manager) activityKey(pairKey string) string {
	return fmt.Sprintf("blnk:hot_pairs:activity:%s", pairKey)
}

func (m *Manager) contentionKey(pairKey string) string {
	return fmt.Sprintf("blnk:hot_pairs:contention:%s", pairKey)
}

func (m *Manager) stateTTL() time.Duration {
	return m.pairTTL * 4
}
