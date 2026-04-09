package hotpairs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/model"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRecordContentionPromotesPair(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	manager := NewManager(client, Config{
		Enabled:                 true,
		HotQueueName:            "hot_transactions",
		HotPairTTL:              time.Minute,
		LockContentionThreshold: 3,
	})

	ctx := context.Background()
	pairKey := manager.PairKey("src", "dst", "usd")

	count, promoted, err := manager.RecordContention(ctx, pairKey)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.False(t, promoted)

	count, promoted, err = manager.RecordContention(ctx, pairKey)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	require.False(t, promoted)

	count, promoted, err = manager.RecordContention(ctx, pairKey)
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
	require.True(t, promoted)

	state, err := manager.GetState(ctx, pairKey)
	require.NoError(t, err)
	require.Equal(t, StatePromoting, state)
}

func TestHotPairLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	manager := NewManager(client, Config{
		Enabled:                 true,
		HotQueueName:            "hot_transactions",
		HotPairTTL:              time.Minute,
		LockContentionThreshold: 2,
	})

	ctx := context.Background()
	pairKey := manager.PairKey("src", "dst", "usd")

	require.NoError(t, manager.ActivateHot(ctx, pairKey))

	state, err := manager.GetState(ctx, pairKey)
	require.NoError(t, err)
	require.Equal(t, StateHot, state)

	active, err := manager.HasRecentContention(ctx, pairKey)
	require.NoError(t, err)
	require.True(t, active)

	require.NoError(t, manager.StartCoolingDown(ctx, pairKey))
	state, err = manager.GetState(ctx, pairKey)
	require.NoError(t, err)
	require.Equal(t, StateCoolingDown, state)

	require.NoError(t, manager.SetNormal(ctx, pairKey))
	state, err = manager.GetState(ctx, pairKey)
	require.NoError(t, err)
	require.Equal(t, StateNormal, state)
}

type stubCounter struct {
	counts map[string]int
}

func (s stubCounter) CountQueuedTransactionsForPairLane(ctx context.Context, source, destination, currency, lane string) (int, error) {
	return s.counts[source+"|"+destination+"|"+currency+"|"+lane], nil
}

func TestAssignQueueLaneDefaultsToNormal(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	manager := NewManager(client, Config{
		Enabled:                 true,
		HotQueueName:            "hot_transactions",
		HotPairTTL:              time.Minute,
		LockContentionThreshold: 3,
	})

	txn := &model.Transaction{
		TransactionID: "txn_1",
		Source:        "src",
		Destination:   "dst",
		Currency:      "USD",
	}

	require.NoError(t, AssignQueueLane(context.Background(), manager, stubCounter{}, txn))
	require.Equal(t, LaneNormal, txn.MetaData[QueueLaneMetaKey])
}

func TestResolveQueueLanePromotesOnlyAfterNormalDrain(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	manager := NewManager(client, Config{
		Enabled:                 true,
		HotQueueName:            "hot_transactions",
		HotPairTTL:              time.Minute,
		LockContentionThreshold: 2,
	})

	ctx := context.Background()
	pairKey := manager.PairKey("src", "dst", "USD")
	_, _, err = manager.RecordContention(ctx, pairKey)
	require.NoError(t, err)
	_, promoted, err := manager.RecordContention(ctx, pairKey)
	require.NoError(t, err)
	require.True(t, promoted)

	counter := stubCounter{
		counts: map[string]int{
			"src|dst|USD|" + LaneNormal: 1,
		},
	}
	lane, err := ResolveQueueLane(ctx, manager, counter, "src", "dst", "USD")
	require.NoError(t, err)
	require.Equal(t, LaneNormal, lane)

	counter.counts["src|dst|USD|"+LaneNormal] = 0
	lane, err = ResolveQueueLane(ctx, manager, counter, "src", "dst", "USD")
	require.NoError(t, err)
	require.Equal(t, LaneHot, lane)
}

func TestQueueLaneFromMetadata(t *testing.T) {
	require.Equal(t, LaneNormal, QueueLaneFromMetadata(nil))
	require.Equal(t, LaneHot, QueueLaneFromMetadata(map[string]interface{}{QueueLaneMetaKey: LaneHot}))
}

func TestIsLockContentionError(t *testing.T) {
	require.True(t, IsLockContentionError(fmt.Errorf("wrapped: %w", redlock.ErrLockHeld)))
	require.True(t, IsLockContentionError(fmt.Errorf("wrapped: %w", redlock.ErrLockWaitTimeout)))
	require.False(t, IsLockContentionError(context.DeadlineExceeded))
}
