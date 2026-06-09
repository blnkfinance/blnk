/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package blnk

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// classifyInflightError is a pure function: a focused unit test verifies
// that each known error string from CommitInflightTransaction /
// VoidInflightTransaction maps to its stable public code. This is what
// callers branch on, so drift here is a silent API break.
func TestClassifyInflightError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"not found", fmt.Errorf("transaction not found"), "NOT_FOUND"},
		{"sql no rows is treated as not found", fmt.Errorf("sql: no rows in result set"), "NOT_FOUND"},
		{"apierror wrapped not found", fmt.Errorf("Transaction with ID 'txn_xyz' not found"), "NOT_FOUND"},
		{"already voided", fmt.Errorf("transaction has already been voided"), "ALREADY_VOIDED"},
		{"already committed (commit path)", errors.New("cannot commit. Transaction already committed"), "ALREADY_COMMITTED"},
		{"already committed (void path)", errors.New("cannot void. Transaction already committed"), "ALREADY_COMMITTED"},
		{"not inflight", fmt.Errorf("transaction is not in inflight status"), "NOT_INFLIGHT"},
		{"over original amount", fmt.Errorf("cannot commit more than the original transaction amount. Original: USD500.00, Requested: USD600.00"), "INVALID_AMOUNT"},
		{"over remaining amount", fmt.Errorf("cannot commit more than the remaining amount. Available: USD100.00, Requested: USD200.00"), "INVALID_AMOUNT"},
		{"lock contention", fmt.Errorf("failed to acquire lock for inflight commit: timeout"), "LOCKED"},
		{"unknown error falls back to internal", errors.New("unexpected database failure"), "INTERNAL_ERROR"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyInflightError(tc.err)
			if got != tc.want {
				t.Errorf("classifyInflightError(%q) = %q, want %q", errString(tc.err), got, tc.want)
			}
		})
	}
}

// TestBulkInflightUpdate_InputValidation exercises the input-validation
// guards on BulkInflightUpdate. These run without any infra because they
// return before touching the DB / lock service.
func TestBulkInflightUpdate_InputValidation(t *testing.T) {
	l := &Blnk{}

	t.Run("empty items returns error", func(t *testing.T) {
		_, err := l.BulkInflightUpdate(context.TODO(), BulkInflightVoid, nil, 4)
		if err == nil {
			t.Fatal("expected error for empty items, got nil")
		}
	})
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
