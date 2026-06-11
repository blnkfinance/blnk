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

package main

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// verifyChainCommands replays the transaction hash chain from genesis and
// reports whether history is intact. Exits non-zero on the first broken link.
func verifyChainCommands(b *blnkInstance) *cobra.Command {
	return &cobra.Command{
		Use:           "verify-chain",
		Short:         "Replay and verify the transaction hash chain from genesis",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := b.blnk.VerifyChain(context.Background(), nil)
			if err != nil {
				return fmt.Errorf("verify-chain failed: %w", err)
			}
			if !result.Verified {
				return fmt.Errorf("chain BROKEN at seq %d (txn %s): %s",
					result.BrokenSeq, result.BrokenTxnID, result.Reason)
			}
			logrus.Infof("chain verified: %d transactions sealed, head %s at seq %d",
				result.LastSeq, result.HeadHash, result.LastSeq)
			return nil
		},
	}
}
