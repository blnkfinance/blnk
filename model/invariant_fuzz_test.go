package model

// Invariant fuzz harness for the ledger's balance arithmetic.
//
// A simulated closed system of balances is driven through randomized
// sequences of the real model operations (UpdateBalances, inflight holds,
// commits, voids, rollbacks), and after every single operation the global
// accounting invariants are re-asserted:
//
//   1. Conservation: sum(CreditBalance) == sum(DebitBalance) and
//      sum(InflightCreditBalance) == sum(InflightDebitBalance) across all
//      balances, so sum(Balance) == 0 and sum(InflightBalance) == 0.
//   2. Component sanity: all four credit/debit components are non-negative.
//   3. Derived-balance identity: Balance == CreditBalance - DebitBalance and
//      InflightBalance == InflightCreditBalance - InflightDebitBalance.
//   4. Overdraft safety: a balance that has only ever sourced transactions
//      without overdraft (or within an overdraft limit L) never has
//      Balance - InflightDebitBalance < 0 (resp. < -L).
//   5. Hold accounting: each balance's inflight pools equal the sum of the
//      harness-tracked remaining amounts of its open holds.
//   6. Rejection atomicity: an operation that returns an error leaves every
//      balance byte-identical to its state before the call.
//
// Two entry points share the engine:
//   - TestLedgerInvariants_Seeded: fixed-seed random sequences, runs in CI
//     (fewer iterations under -short).
//   - FuzzLedgerInvariants: coverage-guided, run with
//     go test -fuzz=FuzzLedgerInvariants ./model
//
// On a violation the failure message includes the trailing operation log and
// a dump of every balance and open hold; for the fuzz target Go additionally
// persists the failing input under testdata/fuzz for replay.

import (
	"fmt"
	"math/big"
	"math/rand"
	"strings"
	"testing"
)

const (
	invParticipants    = 5   // balances 1..5; balance 0 is the external world
	invMaxOps          = 512 // hard cap on operations per sequence
	invOplogTail       = 40  // operations shown in a failure dump
	invSeededSequences = 512
	invSeededShort     = 64
	invSequenceBytes   = 4096
)

// invByteSource deterministically converts a fuzz input into operation
// choices. When the bytes run out it reports done and yields zeros, ending
// the sequence.
type invByteSource struct {
	data []byte
	pos  int
}

func (b *invByteSource) done() bool { return b.pos >= len(b.data) }

func (b *invByteSource) byte() byte {
	if b.done() {
		return 0
	}
	v := b.data[b.pos]
	b.pos++
	return v
}

func (b *invByteSource) uint32() uint32 {
	var v uint32
	for i := 0; i < 4; i++ {
		v = v<<8 | uint32(b.byte())
	}
	return v
}

func (b *invByteSource) uint64() uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v = v<<8 | uint64(b.byte())
	}
	return v
}

func (b *invByteSource) intn(n int) int {
	return int(b.uint32() % uint32(n))
}

// amount yields a positive amount across several magnitude bands, including
// values beyond int64 range, so the arithmetic is exercised well past the
// scales unit tests reach for.
func (b *invByteSource) amount() *big.Int {
	switch b.byte() % 4 {
	case 0:
		return big.NewInt(int64(b.byte()%100) + 1)
	case 1:
		return big.NewInt(int64(b.uint32()%1_000_000) + 1)
	case 2:
		return big.NewInt(int64(b.uint64()%(1<<40)) + 1)
	default:
		v := new(big.Int).SetUint64(b.uint64())
		v.Lsh(v, uint(b.byte()%16))
		return v.Add(v, big.NewInt(1))
	}
}

// amountBelow yields an amount in [1, max]. Returns nil when max <= 0.
func (b *invByteSource) amountBelow(max *big.Int) *big.Int {
	if max.Sign() <= 0 {
		return nil
	}
	r := new(big.Int).SetUint64(b.uint64())
	r.Mod(r, max)
	return r.Add(r, big.NewInt(1))
}

// invHold is the harness's own record of an open inflight hold: what the
// model's inflight pools must add up to.
type invHold struct {
	src, dst  int
	remaining *big.Int
}

// invSim is a closed ledger system: balance 0 is the unbounded external
// world that funds the others; balances 1..invParticipants transact among
// themselves.
type invSim struct {
	t        *testing.T
	balances []*Balance
	// maxOverdraft[i] is the largest overdraft limit (in precise units) that
	// balance i has ever successfully sourced a transaction under; nil means
	// unbounded (an unconditional-overdraft transaction touched it).
	maxOverdraft []*big.Int
	holds        []*invHold
	oplog        []string
}

func newInvSim(t *testing.T) *invSim {
	s := &invSim{t: t}
	for i := 0; i <= invParticipants; i++ {
		b := &Balance{BalanceID: fmt.Sprintf("bln_sim_%d", i), Currency: "SIM"}
		b.InitializeBalanceFields()
		s.balances = append(s.balances, b)
		s.maxOverdraft = append(s.maxOverdraft, big.NewInt(0))
	}
	s.maxOverdraft[0] = nil // the world funds everyone via overdraft
	return s
}

func (s *invSim) logf(format string, args ...interface{}) {
	s.oplog = append(s.oplog, fmt.Sprintf(format, args...))
}

func (s *invSim) failf(format string, args ...interface{}) {
	s.t.Helper()
	var sb strings.Builder
	fmt.Fprintf(&sb, format, args...)
	sb.WriteString("\n\n--- operation log (most recent last) ---\n")
	start := len(s.oplog) - invOplogTail
	if start < 0 {
		start = 0
	}
	for i := start; i < len(s.oplog); i++ {
		fmt.Fprintf(&sb, "%4d: %s\n", i, s.oplog[i])
	}
	sb.WriteString("--- balances ---\n")
	for i, b := range s.balances {
		od := "unbounded"
		if s.maxOverdraft[i] != nil {
			od = s.maxOverdraft[i].String()
		}
		fmt.Fprintf(&sb, "[%d] bal=%s credit=%s debit=%s ibal=%s icredit=%s idebit=%s maxOD=%s\n",
			i, b.Balance, b.CreditBalance, b.DebitBalance,
			b.InflightBalance, b.InflightCreditBalance, b.InflightDebitBalance, od)
	}
	sb.WriteString("--- open holds ---\n")
	for i, h := range s.holds {
		if h.remaining.Sign() > 0 {
			fmt.Fprintf(&sb, "hold %d: %d -> %d remaining=%s\n", i, h.src, h.dst, h.remaining)
		}
	}
	s.t.Fatal(sb.String())
}

// available mirrors canProcessTransaction's funds check (no queued balances
// in this simulation): committed balance minus inflight debits.
func (s *invSim) available(i int) *big.Int {
	b := s.balances[i]
	return new(big.Int).Sub(b.Balance, b.InflightDebitBalance)
}

func (s *invSim) raiseOverdraft(i int, limit *big.Int) {
	if s.maxOverdraft[i] == nil {
		return
	}
	if limit == nil {
		s.maxOverdraft[i] = nil
		return
	}
	if s.maxOverdraft[i].Cmp(limit) < 0 {
		s.maxOverdraft[i] = new(big.Int).Set(limit)
	}
}

func (s *invSim) pickParticipant(bs *invByteSource) int {
	return 1 + bs.intn(invParticipants)
}

func (s *invSim) pickPair(bs *invByteSource) (int, int) {
	src := s.pickParticipant(bs)
	offset := 1 + bs.intn(invParticipants-1)
	dst := 1 + ((src - 1 + offset) % invParticipants)
	return src, dst
}

// pickLiveHold returns an open hold or nil when none exist.
func (s *invSim) pickLiveHold(bs *invByteSource) *invHold {
	var live []*invHold
	for _, h := range s.holds {
		if h.remaining.Sign() > 0 {
			live = append(live, h)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[bs.intn(len(live))]
}

type invSnapshot [][6]string

func (s *invSim) snapshot() invSnapshot {
	snap := make(invSnapshot, len(s.balances))
	for i, b := range s.balances {
		snap[i] = [6]string{
			b.Balance.String(), b.CreditBalance.String(), b.DebitBalance.String(),
			b.InflightBalance.String(), b.InflightCreditBalance.String(), b.InflightDebitBalance.String(),
		}
	}
	return snap
}

func (s *invSim) assertUnchanged(before invSnapshot, op string) {
	s.t.Helper()
	if fmt.Sprint(before) != fmt.Sprint(s.snapshot()) {
		s.failf("%s: rejected operation mutated balance state", op)
	}
}

// assertInvariants re-checks every global invariant; called after each step.
func (s *invSim) assertInvariants() {
	s.t.Helper()
	sumCredit, sumDebit := new(big.Int), new(big.Int)
	sumICredit, sumIDebit := new(big.Int), new(big.Int)
	sumBal, sumIBal := new(big.Int), new(big.Int)

	heldOut := make([]*big.Int, len(s.balances))
	heldIn := make([]*big.Int, len(s.balances))
	for i := range s.balances {
		heldOut[i], heldIn[i] = new(big.Int), new(big.Int)
	}
	for _, h := range s.holds {
		heldOut[h.src].Add(heldOut[h.src], h.remaining)
		heldIn[h.dst].Add(heldIn[h.dst], h.remaining)
	}

	for i, b := range s.balances {
		for name, v := range map[string]*big.Int{
			"credit_balance":          b.CreditBalance,
			"debit_balance":           b.DebitBalance,
			"inflight_credit_balance": b.InflightCreditBalance,
			"inflight_debit_balance":  b.InflightDebitBalance,
		} {
			if v.Sign() < 0 {
				s.failf("invariant: balance %d has negative %s = %s", i, name, v)
			}
		}

		if got := new(big.Int).Sub(b.CreditBalance, b.DebitBalance); got.Cmp(b.Balance) != 0 {
			s.failf("invariant: balance %d Balance=%s but credit-debit=%s", i, b.Balance, got)
		}
		if got := new(big.Int).Sub(b.InflightCreditBalance, b.InflightDebitBalance); got.Cmp(b.InflightBalance) != 0 {
			s.failf("invariant: balance %d InflightBalance=%s but icredit-idebit=%s", i, b.InflightBalance, got)
		}

		if b.InflightDebitBalance.Cmp(heldOut[i]) != 0 {
			s.failf("invariant: balance %d inflight_debit_balance=%s but open holds sum to %s",
				i, b.InflightDebitBalance, heldOut[i])
		}
		if b.InflightCreditBalance.Cmp(heldIn[i]) != 0 {
			s.failf("invariant: balance %d inflight_credit_balance=%s but open holds sum to %s",
				i, b.InflightCreditBalance, heldIn[i])
		}

		if s.maxOverdraft[i] != nil {
			floor := new(big.Int).Neg(s.maxOverdraft[i])
			if avail := s.available(i); avail.Cmp(floor) < 0 {
				s.failf("invariant: balance %d available=%s below overdraft floor %s", i, avail, floor)
			}
		}

		sumCredit.Add(sumCredit, b.CreditBalance)
		sumDebit.Add(sumDebit, b.DebitBalance)
		sumICredit.Add(sumICredit, b.InflightCreditBalance)
		sumIDebit.Add(sumIDebit, b.InflightDebitBalance)
		sumBal.Add(sumBal, b.Balance)
		sumIBal.Add(sumIBal, b.InflightBalance)
	}

	if sumCredit.Cmp(sumDebit) != 0 {
		s.failf("invariant: money not conserved: sum(credit)=%s sum(debit)=%s", sumCredit, sumDebit)
	}
	if sumICredit.Cmp(sumIDebit) != 0 {
		s.failf("invariant: inflight money not conserved: sum(icredit)=%s sum(idebit)=%s", sumICredit, sumIDebit)
	}
	if sumBal.Sign() != 0 {
		s.failf("invariant: closed system sum(balance)=%s, want 0", sumBal)
	}
	if sumIBal.Sign() != 0 {
		s.failf("invariant: closed system sum(inflight_balance)=%s, want 0", sumIBal)
	}
}

// --- operations -------------------------------------------------------------

// opFund credits a participant from the unbounded world balance.
func (s *invSim) opFund(bs *invByteSource) {
	dst := s.pickParticipant(bs)
	amt := bs.amount()
	s.logf("fund world->%d amount=%s", dst, amt)
	txn := &Transaction{PreciseAmount: amt, Precision: 1, AllowOverdraft: true}
	if err := UpdateBalances(txn, s.balances[0], s.balances[dst]); err != nil {
		s.failf("fund: unexpected rejection: %v", err)
	}
}

// opTransferWithin moves an amount the source can cover; the model must
// accept it.
func (s *invSim) opTransferWithin(bs *invByteSource, inflight bool) {
	src, dst := s.pickPair(bs)
	amt := bs.amountBelow(s.available(src))
	if amt == nil {
		s.logf("transfer-within %d->%d skipped (no available funds)", src, dst)
		return
	}
	s.logf("transfer-within %d->%d amount=%s inflight=%v", src, dst, amt, inflight)
	txn := &Transaction{PreciseAmount: amt, Precision: 1, Inflight: inflight}
	if err := UpdateBalances(txn, s.balances[src], s.balances[dst]); err != nil {
		s.failf("transfer within available funds rejected: %v", err)
	}
	if inflight {
		s.holds = append(s.holds, &invHold{src: src, dst: dst, remaining: new(big.Int).Set(txn.PreciseAmount)})
	}
}

// opTransferOverdraft moves an arbitrary amount under unconditional
// overdraft; the source is thereafter exempt from the overdraft-floor check.
func (s *invSim) opTransferOverdraft(bs *invByteSource, inflight bool) {
	src, dst := s.pickPair(bs)
	amt := bs.amount()
	s.logf("transfer-overdraft %d->%d amount=%s inflight=%v", src, dst, amt, inflight)
	txn := &Transaction{PreciseAmount: amt, Precision: 1, AllowOverdraft: true, Inflight: inflight}
	if err := UpdateBalances(txn, s.balances[src], s.balances[dst]); err != nil {
		s.failf("unconditional overdraft transfer rejected: %v", err)
	}
	s.raiseOverdraft(src, nil)
	if inflight {
		s.holds = append(s.holds, &invHold{src: src, dst: dst, remaining: new(big.Int).Set(txn.PreciseAmount)})
	}
}

// opTransferLimited attempts a transfer under a bounded overdraft limit and
// checks the model's accept/reject decision against the harness's own
// arithmetic.
func (s *invSim) opTransferLimited(bs *invByteSource) {
	src, dst := s.pickPair(bs)
	limit := big.NewInt(int64(bs.uint32()%1_000_000) + 1)
	amt := bs.amount()
	s.logf("transfer-limited %d->%d amount=%s limit=%s", src, dst, amt, limit)
	txn := &Transaction{
		PreciseAmount:  amt,
		Precision:      1,
		AllowOverdraft: true,
		OverdraftLimit: float64(limit.Int64()),
	}
	resulting := new(big.Int).Sub(s.available(src), amt)
	expectOK := resulting.Cmp(new(big.Int).Neg(limit)) >= 0

	before := s.snapshot()
	err := UpdateBalances(txn, s.balances[src], s.balances[dst])
	if expectOK && err != nil {
		s.failf("transfer within overdraft limit %s rejected: %v", limit, err)
	}
	if !expectOK {
		if err == nil {
			s.failf("transfer past overdraft limit %s accepted (resulting available %s)", limit, resulting)
		}
		s.assertUnchanged(before, "transfer-limited")
		return
	}
	s.raiseOverdraft(src, limit)
}

// opInsufficientProbe attempts a transfer the source cannot cover with no
// overdraft; the model must reject it without mutating anything.
func (s *invSim) opInsufficientProbe(bs *invByteSource) {
	src, dst := s.pickPair(bs)
	avail := s.available(src)
	amt := big.NewInt(int64(bs.byte()%100) + 1)
	if avail.Sign() > 0 {
		amt.Add(avail, amt)
	}
	s.logf("insufficient-probe %d->%d amount=%s available=%s", src, dst, amt, avail)
	txn := &Transaction{PreciseAmount: amt, Precision: 1}
	before := s.snapshot()
	if err := UpdateBalances(txn, s.balances[src], s.balances[dst]); err == nil {
		s.failf("transfer of %s accepted with only %s available and no overdraft", amt, avail)
	}
	s.assertUnchanged(before, "insufficient-probe")
}

// opCommit settles part or all of an open hold; both legs must succeed.
func (s *invSim) opCommit(bs *invByteSource, full bool) {
	h := s.pickLiveHold(bs)
	if h == nil {
		s.logf("commit skipped (no open holds)")
		return
	}
	amt := new(big.Int).Set(h.remaining)
	if !full {
		amt = bs.amountBelow(h.remaining)
	}
	s.logf("commit %d->%d amount=%s of remaining=%s full=%v", h.src, h.dst, amt, h.remaining, full)
	txn := &Transaction{PreciseAmount: new(big.Int).Set(amt), Precision: 1}
	if err := s.balances[h.src].CommitInflightDebit(txn); err != nil {
		s.failf("commit debit leg rejected with %s held: %v", h.remaining, err)
	}
	if err := s.balances[h.dst].CommitInflightCredit(txn); err != nil {
		s.failf("commit credit leg rejected with %s held: %v", h.remaining, err)
	}
	h.remaining.Sub(h.remaining, amt)
}

// opVoid releases part or all of an open hold; both legs must succeed.
func (s *invSim) opVoid(bs *invByteSource, full bool) {
	h := s.pickLiveHold(bs)
	if h == nil {
		s.logf("void skipped (no open holds)")
		return
	}
	amt := new(big.Int).Set(h.remaining)
	if !full {
		amt = bs.amountBelow(h.remaining)
	}
	s.logf("void %d->%d amount=%s of remaining=%s full=%v", h.src, h.dst, amt, h.remaining, full)
	if err := s.balances[h.src].RollbackInflightDebit(amt); err != nil {
		s.failf("void debit leg rejected with %s held: %v", h.remaining, err)
	}
	if err := s.balances[h.dst].RollbackInflightCredit(amt); err != nil {
		s.failf("void credit leg rejected with %s held: %v", h.remaining, err)
	}
	h.remaining.Sub(h.remaining, amt)
}

// opExcessSettleProbe attempts to commit or roll back more than a balance
// holds inflight; the model must reject it without mutating anything.
func (s *invSim) opExcessSettleProbe(bs *invByteSource) {
	i := s.pickParticipant(bs)
	b := s.balances[i]
	kind := bs.byte() % 3
	before := s.snapshot()
	switch kind {
	case 0:
		amt := new(big.Int).Add(b.InflightDebitBalance, big.NewInt(int64(bs.byte()%100)+1))
		s.logf("excess-commit-probe balance=%d amount=%s pool=%s", i, amt, b.InflightDebitBalance)
		if err := b.CommitInflightDebit(&Transaction{PreciseAmount: amt, Precision: 1}); err == nil {
			s.failf("commit of %s accepted with only %s inflight debit", amt, before[i][5])
		}
	case 1:
		amt := new(big.Int).Add(b.InflightDebitBalance, big.NewInt(int64(bs.byte()%100)+1))
		s.logf("excess-void-debit-probe balance=%d amount=%s pool=%s", i, amt, b.InflightDebitBalance)
		if err := b.RollbackInflightDebit(amt); err == nil {
			s.failf("rollback of %s accepted with only %s inflight debit", amt, before[i][5])
		}
	default:
		amt := new(big.Int).Add(b.InflightCreditBalance, big.NewInt(int64(bs.byte()%100)+1))
		s.logf("excess-void-credit-probe balance=%d amount=%s pool=%s", i, amt, b.InflightCreditBalance)
		if err := b.RollbackInflightCredit(amt); err == nil {
			s.failf("rollback of %s accepted with only %s inflight credit", amt, before[i][4])
		}
	}
	s.assertUnchanged(before, "excess-settle-probe")
}

// opFloatPath drives the float-Amount / Precision conversion path and checks
// the resulting precise amount is exactly the intended minor units — a
// silent-parse-to-zero or rounding drift here fails loudly.
func (s *invSim) opFloatPath(bs *invByteSource) {
	src, dst := s.pickPair(bs)
	minor := int64(bs.uint64()%(1<<40)) + 1
	s.logf("float-path %d->%d minor=%d", src, dst, minor)
	txn := &Transaction{
		Amount:         float64(minor) / 100,
		Precision:      100,
		AllowOverdraft: true,
	}
	if err := UpdateBalances(txn, s.balances[src], s.balances[dst]); err != nil {
		s.failf("float-path transfer rejected: %v", err)
	}
	if txn.PreciseAmount.Cmp(big.NewInt(minor)) != 0 {
		s.failf("float-path precision drift: amount %v at precision 100 became %s precise units, want %d",
			txn.Amount, txn.PreciseAmount, minor)
	}
	s.raiseOverdraft(src, nil)
}

// opNonPositiveProbe attempts zero and negative amounts; validation must
// reject them before any balance is touched.
func (s *invSim) opNonPositiveProbe(bs *invByteSource) {
	src, dst := s.pickPair(bs)
	txn := &Transaction{Precision: 1, AllowOverdraft: true}
	if bs.byte()%2 == 0 {
		txn.PreciseAmount = big.NewInt(-int64(bs.byte()%100) - 1)
	}
	s.logf("non-positive-probe %d->%d precise=%v", src, dst, txn.PreciseAmount)
	before := s.snapshot()
	if err := UpdateBalances(txn, s.balances[src], s.balances[dst]); err == nil {
		s.failf("non-positive amount accepted: precise=%v amount=%v", txn.PreciseAmount, txn.Amount)
	}
	s.assertUnchanged(before, "non-positive-probe")
}

// step executes one randomized operation and re-asserts every invariant.
func (s *invSim) step(bs *invByteSource) {
	switch bs.byte() % 14 {
	case 0, 1:
		s.opFund(bs)
	case 2:
		s.opTransferWithin(bs, false)
	case 3:
		s.opTransferOverdraft(bs, false)
	case 4:
		s.opTransferLimited(bs)
	case 5:
		s.opInsufficientProbe(bs)
	case 6:
		s.opTransferWithin(bs, true)
	case 7:
		s.opTransferOverdraft(bs, true)
	case 8:
		s.opCommit(bs, true)
	case 9:
		s.opCommit(bs, false)
	case 10:
		s.opVoid(bs, true)
	case 11:
		s.opVoid(bs, false)
	case 12:
		s.opExcessSettleProbe(bs)
	default:
		switch bs.byte() % 2 {
		case 0:
			s.opFloatPath(bs)
		default:
			s.opNonPositiveProbe(bs)
		}
	}
	s.assertInvariants()
}

func runInvariantSequence(t *testing.T, data []byte) {
	bs := &invByteSource{data: data}
	s := newInvSim(t)
	for step := 0; step < invMaxOps && !bs.done(); step++ {
		s.step(bs)
	}
}

// TestLedgerInvariants_Seeded runs fixed-seed randomized sequences in normal
// CI runs; a failing seed reproduces exactly via its subtest name.
func TestLedgerInvariants_Seeded(t *testing.T) {
	sequences := invSeededSequences
	if testing.Short() {
		sequences = invSeededShort
	}
	for seed := int64(0); seed < int64(sequences); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			data := make([]byte, invSequenceBytes)
			rng.Read(data)
			runInvariantSequence(t, data)
		})
	}
}

// FuzzLedgerInvariants lets Go's coverage-guided fuzzer hunt for operation
// sequences that break the accounting invariants:
//
//	go test -fuzz=FuzzLedgerInvariants -fuzztime=60s ./model
func FuzzLedgerInvariants(f *testing.F) {
	f.Add(make([]byte, 256))
	ramp := make([]byte, 1024)
	for i := range ramp {
		ramp[i] = byte(i * 7)
	}
	f.Add(ramp)
	seedRng := rand.New(rand.NewSource(42))
	seeded := make([]byte, 2048)
	seedRng.Read(seeded)
	f.Add(seeded)

	f.Fuzz(func(t *testing.T, data []byte) {
		runInvariantSequence(t, data)
	})
}
