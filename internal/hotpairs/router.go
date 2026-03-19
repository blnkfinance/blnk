package hotpairs

import (
	"context"
	"strings"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

const (
	QueueLaneMetaKey = "queue_lane"
	QueuePairMetaKey = "queue_pair_key"
)

type PairLaneCounter interface {
	CountQueuedTransactionsForPairLane(ctx context.Context, source, destination, currency, lane string) (int, error)
}

func AssignQueueLane(ctx context.Context, manager *Manager, counter PairLaneCounter, txn *model.Transaction) error {
	if txn == nil {
		return nil
	}
	if txn.MetaData == nil {
		txn.MetaData = make(map[string]interface{})
	}

	lane := LaneNormal
	pairKey := PairKeyForTransaction(manager, txn)
	if pairKey != "" {
		resolvedLane, err := ResolveQueueLane(ctx, manager, counter, txn.Source, txn.Destination, txn.Currency)
		if err != nil {
			logrus.WithError(err).WithField("transaction_id", txn.TransactionID).Warn("failed to resolve hot-pair lane, using normal lane")
		} else {
			lane = resolvedLane
		}
		txn.MetaData[QueuePairMetaKey] = pairKey
	}

	txn.MetaData[QueueLaneMetaKey] = lane
	return nil
}

func ResolveQueueLane(ctx context.Context, manager *Manager, counter PairLaneCounter, source, destination, currency string) (string, error) {
	if manager == nil || !manager.Enabled() {
		return LaneNormal, nil
	}

	pairKey := manager.PairKey(source, destination, currency)
	state, err := manager.GetState(ctx, pairKey)
	if err != nil {
		return LaneNormal, err
	}

	switch state {
	case StatePromoting:
		pendingNormal, err := pendingQueuedTransactionsForPairLane(ctx, counter, source, destination, currency, LaneNormal)
		if err != nil {
			return LaneNormal, err
		}
		if pendingNormal == 0 {
			if err := manager.ActivateHot(ctx, pairKey); err != nil {
				return LaneNormal, err
			}
			logrus.WithFields(logrus.Fields{
				"pair_key": pairKey,
				"source":   source,
				"dest":     destination,
				"currency": strings.ToUpper(currency),
			}).Info("Promoted hot pair to hot queue")
			return LaneHot, nil
		}
		return LaneNormal, nil
	case StateHot:
		active, err := manager.HasRecentContention(ctx, pairKey)
		if err != nil {
			return LaneNormal, err
		}
		if active {
			return LaneHot, nil
		}

		pendingHot, err := pendingQueuedTransactionsForPairLane(ctx, counter, source, destination, currency, LaneHot)
		if err != nil {
			return LaneNormal, err
		}
		if pendingHot > 0 {
			if err := manager.StartCoolingDown(ctx, pairKey); err != nil {
				return LaneNormal, err
			}
			return LaneHot, nil
		}

		if err := manager.SetNormal(ctx, pairKey); err != nil {
			return LaneNormal, err
		}
		logrus.WithField("pair_key", pairKey).Info("Demoted hot pair back to normal queue")
		return LaneNormal, nil
	case StateCoolingDown:
		pendingHot, err := pendingQueuedTransactionsForPairLane(ctx, counter, source, destination, currency, LaneHot)
		if err != nil {
			return LaneNormal, err
		}
		if pendingHot > 0 {
			return LaneHot, nil
		}
		if err := manager.SetNormal(ctx, pairKey); err != nil {
			return LaneNormal, err
		}
		logrus.WithField("pair_key", pairKey).Info("Completed hot-pair cooldown; returning to normal queue")
		return LaneNormal, nil
	default:
		return LaneNormal, nil
	}
}

func PairKeyForTransaction(manager *Manager, txn *model.Transaction) string {
	if txn == nil || manager == nil || txn.Source == "" || txn.Destination == "" || txn.Currency == "" {
		return ""
	}
	return manager.PairKey(txn.Source, txn.Destination, txn.Currency)
}

func QueueLaneFromMetadata(meta map[string]interface{}) string {
	if meta == nil {
		return LaneNormal
	}
	lane, _ := meta[QueueLaneMetaKey].(string)
	if lane == LaneHot {
		return LaneHot
	}
	return LaneNormal
}

func RecordContention(ctx context.Context, manager *Manager, source, destination, currency string, err error) {
	if !IsLockContentionError(err) || manager == nil || !manager.Enabled() {
		return
	}
	if source == "" || destination == "" || currency == "" {
		return
	}

	pairKey := manager.PairKey(source, destination, currency)
	count, promoted, recordErr := manager.RecordContention(ctx, pairKey)
	if recordErr != nil {
		logrus.WithError(recordErr).WithField("pair_key", pairKey).Warn("failed to record hot-pair contention")
		return
	}

	fields := logrus.Fields{
		"pair_key":    pairKey,
		"source":      source,
		"destination": destination,
		"currency":    strings.ToUpper(currency),
		"count":       count,
	}
	if promoted {
		logrus.WithFields(fields).Info("Hot-pair contention threshold reached; pair marked for promotion")
		return
	}
}

func IsLockContentionError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already held") ||
		strings.Contains(msg, "failed to acquire lock") ||
		strings.Contains(msg, "failed to acquire batch lock")
}

func pendingQueuedTransactionsForPairLane(ctx context.Context, counter PairLaneCounter, source, destination, currency, lane string) (int, error) {
	if counter == nil {
		return 0, nil
	}
	return counter.CountQueuedTransactionsForPairLane(ctx, source, destination, currency, lane)
}
