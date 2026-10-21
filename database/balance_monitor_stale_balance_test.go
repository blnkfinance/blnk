package database

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// A monitor's list is cached per balance, so for a few minutes after a monitor
// is moved a node can still evaluate it against the balance it used to watch.
// Versions count per balance, so without the balance guard that stale
// evaluation pins state_version above anything the monitor's own balance will
// reach for thousands of transactions, and the monitor goes silent.
func TestTransitionMonitorState_IgnoresAnotherBalance_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()
	_, busy := seedMonitor(t, ds)  // stands in for an old, busy balance
	_, fresh := seedMonitor(t, ds) // and a freshly created one

	monitor, err := ds.CreateMonitor(model.BalanceMonitor{
		BalanceID: busy,
		Condition: model.AlertCondition{Field: "balance", Operator: "<", Value: 100, Precision: 1, PreciseValue: big.NewInt(100)},
	})
	require.NoError(t, err)

	// The operator moves the monitor to the fresh balance. This re-arms it.
	monitor.BalanceID = fresh
	require.NoError(t, ds.UpdateMonitor(&monitor))

	// A node whose cached monitor list for the busy balance has not expired yet
	// evaluates the monitor against the balance it no longer watches.
	_, err = ds.TransitionMonitorState(ctx, monitor.MonitorID, busy, true, 5000)
	require.NoError(t, err)

	// Now a genuine crossing on the balance the monitor actually watches.
	owned, err := ds.TransitionMonitorState(ctx, monitor.MonitorID, fresh, true, 3)
	require.NoError(t, err)
	require.True(t, owned, "a real crossing on the monitor's own balance must not be fenced out by an evaluation for a different balance")
}
