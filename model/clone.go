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

import "math/big"

// cloneBigInt copies v by value.
//
// nil is preserved rather than promoted to zero so that a clone behaves
// identically to its original under InitializeBalanceFields, which
// distinguishes "unset" from "zero".
func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

// Clone returns a deep copy of the balance that a transaction can be applied
// to without affecting the original.
//
// Every *big.Int field is copied by value. This is what makes the copy
// meaningful: big.Int's Add and Sub mutate their receiver, so a plain struct
// copy would share the underlying integers with the original, and applying a
// transaction to the copy would silently rewrite the original's balances too.
//
// Used by the dry-run projection to hold a "before" snapshot alongside the
// balances the transaction is applied to.
func (balance *Balance) Clone() *Balance {
	if balance == nil {
		return nil
	}

	clone := *balance

	clone.Balance = cloneBigInt(balance.Balance)
	clone.InflightBalance = cloneBigInt(balance.InflightBalance)
	clone.CreditBalance = cloneBigInt(balance.CreditBalance)
	clone.InflightCreditBalance = cloneBigInt(balance.InflightCreditBalance)
	clone.DebitBalance = cloneBigInt(balance.DebitBalance)
	clone.InflightDebitBalance = cloneBigInt(balance.InflightDebitBalance)
	clone.QueuedDebitBalance = cloneBigInt(balance.QueuedDebitBalance)
	clone.QueuedCreditBalance = cloneBigInt(balance.QueuedCreditBalance)

	if balance.MetaData != nil {
		metaData := make(map[string]interface{}, len(balance.MetaData))
		for k, v := range balance.MetaData {
			metaData[k] = v
		}
		clone.MetaData = metaData
	}

	return &clone
}

// Clone returns a deep copy of the transaction, safe to mutate without
// affecting the original.
//
// The balance-applying path mutates the transaction it is given — UpdateBalances
// sets PreciseAmount, and the inflight helpers rewrite Amount — so a caller that
// wants to keep its own transaction intact (notably the dry-run projection, which
// must not disturb a transaction it was only asked to evaluate) applies the
// arithmetic to a clone.
func (transaction *Transaction) Clone() *Transaction {
	if transaction == nil {
		return nil
	}

	clone := *transaction

	clone.PreciseAmount = cloneBigInt(transaction.PreciseAmount)

	if transaction.EffectiveDate != nil {
		effectiveDate := *transaction.EffectiveDate
		clone.EffectiveDate = &effectiveDate
	}

	if transaction.MetaData != nil {
		metaData := make(map[string]interface{}, len(transaction.MetaData))
		for k, v := range transaction.MetaData {
			metaData[k] = v
		}
		clone.MetaData = metaData
	}

	if transaction.Sources != nil {
		clone.Sources = make([]Distribution, len(transaction.Sources))
		copy(clone.Sources, transaction.Sources)
	}

	if transaction.Destinations != nil {
		clone.Destinations = make([]Distribution, len(transaction.Destinations))
		copy(clone.Destinations, transaction.Destinations)
	}

	if transaction.GroupIds != nil {
		clone.GroupIds = make([]string, len(transaction.GroupIds))
		copy(clone.GroupIds, transaction.GroupIds)
	}

	return &clone
}
