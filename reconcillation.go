package blnk

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/texttheater/golang-levenshtein/levenshtein"
)

const (
	StatusStarted    = "started"
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

func detectFileType(data []byte) (string, error) {
	if looksLikeCSV(data) {
		return "csv", nil
	}

	if looksLikeJSON(data) {
		return "json", nil
	}

	return "", fmt.Errorf("unable to detect file type")
}

func looksLikeCSV(data []byte) bool {
	// Simple heuristic: check if the first line contains commas
	firstLine := bytes.SplitN(data, []byte("\n"), 2)[0]
	return bytes.Count(firstLine, []byte(",")) > 0
}

func looksLikeJSON(data []byte) bool {
	return json.Valid(data)
}

func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0 // Return 0 if parsing fails
	}
	return f
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{} // Return zero time if parsing fails
	}
	return t
}

func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func (s *Blnk) parseAndStoreCSV(ctx context.Context, uploadID, source string, reader io.Reader) error {
	csvReader := csv.NewReader(reader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return err
	}

	// Assuming the first row is headers
	for _, record := range records[1:] {
		externalTxn := model.ExternalTransaction{
			ID:     record[0],
			Amount: parseFloat(record[1]),
			Date:   parseTime(record[2]),
			Source: source,
		}
		if err := s.storeExternalTransaction(ctx, uploadID, externalTxn); err != nil {
			return err
		}
	}

	return nil
}

func (s *Blnk) parseAndStoreJSON(ctx context.Context, uploadID, source string, reader io.Reader) error {
	decoder := json.NewDecoder(reader)
	var transactions []model.ExternalTransaction
	if err := decoder.Decode(&transactions); err != nil {
		return err
	}

	fmt.Println("uploded transactions", len(transactions))
	for _, txn := range transactions {
		txn.Source = source
		if err := s.storeExternalTransaction(ctx, uploadID, txn); err != nil {
			return err
		}
	}

	return nil
}

func (s *Blnk) UploadExternalData(ctx context.Context, source string, reader io.Reader) (string, error) {
	uploadID := model.GenerateUUIDWithSuffix("upload")

	// Read the entire content into a buffer
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return "", fmt.Errorf("error reading upload data: %w", err)
	}

	// Detect the file type
	fileType, err := detectFileType(buf.Bytes())
	if err != nil {
		return "", fmt.Errorf("error detecting file type: %w", err)
	}

	// Parse and store the data based on the detected file type
	switch fileType {
	case "csv":
		err = s.parseAndStoreCSV(ctx, uploadID, source, &buf)
	case "json":
		err = s.parseAndStoreJSON(ctx, uploadID, source, &buf)
	default:
		return "", fmt.Errorf("unsupported file type: %s", fileType)
	}

	if err != nil {
		return "", err
	}

	return uploadID, nil
}

func (s *Blnk) storeExternalTransaction(ctx context.Context, uploadID string, txn model.ExternalTransaction) error {
	return s.datasource.RecordExternalTransaction(ctx, &txn, uploadID)
}

func (l *Blnk) postReconciliationActions(_ context.Context, reconciliation model.Reconciliation) {
	go func() {
		err := l.queue.queueIndexData(reconciliation.ReconciliationID, "reconciliations", reconciliation)
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

func (s *Blnk) StartReconciliation(ctx context.Context, uploadID string, strategy string, groupingCriteria map[string]interface{}, matchingRuleIDs []string, isDryRun bool) (string, error) {
	reconciliationID := model.GenerateUUIDWithSuffix("recon")
	reconciliation := model.Reconciliation{
		ReconciliationID: reconciliationID,
		UploadID:         uploadID,
		Status:           StatusStarted,
		StartedAt:        time.Now(),
		IsDryRun:         isDryRun,
	}

	if err := s.datasource.RecordReconciliation(ctx, &reconciliation); err != nil {
		return "", err
	}

	// Start the reconciliation process in a goroutine
	go func() {
		err := s.processReconciliation(context.Background(), reconciliation, strategy, groupingCriteria, matchingRuleIDs)
		if err != nil {
			log.Printf("Error in reconciliation process: %v", err)
			err := s.datasource.UpdateReconciliationStatus(context.Background(), reconciliationID, StatusFailed, 0, 0)
			if err != nil {
				log.Printf("Error updating reconciliation status: %v", err)
			}
		}
	}()

	return reconciliationID, nil
}

func (s *Blnk) storeMatches(ctx context.Context, reconciliationID string, matches []model.Match) error {
	for _, match := range matches {
		match.ReconciliationID = reconciliationID
		return s.datasource.RecordMatch(ctx, &match)
	}
	return nil
}

// Update the existing matchesRules method to use model.MatchingRule
func (s *Blnk) matchesRules(externalTxn *model.Transaction, internalTxn model.Transaction, rules []model.MatchingRule) bool {
	for _, rule := range rules {
		allCriteriaMet := true
		for _, criteria := range rule.Criteria {
			if !s.matchesCriteria(externalTxn, internalTxn, criteria) {
				allCriteriaMet = false
				break
			}
		}
		if allCriteriaMet {
			return true
		}
	}
	return false
}

func (s *Blnk) saveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	return s.datasource.SaveReconciliationProgress(ctx, reconciliationID, progress)
}

func (s *Blnk) loadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	return s.datasource.LoadReconciliationProgress(ctx, reconciliationID)
}

func (s *Blnk) processReconciliation(ctx context.Context, reconciliation model.Reconciliation, strategy string, groupingCriteria map[string]interface{}, matchingRuleIDs []string) error {
	err := s.datasource.UpdateReconciliationStatus(ctx, reconciliation.ReconciliationID, StatusInProgress, 0, 0)
	if err != nil {
		log.Printf("Error updating reconciliation status: %v", err)
	}

	matchingRules, err := s.getMatchingRules(ctx, matchingRuleIDs)
	if err != nil {
		return err
	}

	var matches []model.Match
	var unmatched []string

	progress, err := s.loadReconciliationProgress(ctx, reconciliation.ReconciliationID)
	if err != nil {
		log.Printf("Error loading reconciliation progress: %v", err)
		progress = model.ReconciliationProgress{} // Start from beginning if unable to load progress
	}

	_, err = s.ProcessTransactionInBatches(
		ctx,
		reconciliation.UploadID,
		0, // amount is not relevant for external transactions processing
		s.getExternalTransactionsPaginated,
		func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, _ float64) {
			defer wg.Done()
			for externalTxn := range jobs {

				//TODO: external transaction id is not sequential, so we need to find a way to skip already processed transactions
				// Skip already processed transactions
				if externalTxn.TransactionID <= progress.LastProcessedExternalTxnID {
					continue
				}

				var batchMatches []model.Match
				var batchUnmatched []string

				switch strategy {
				case "one_to_one":
					batchMatches, batchUnmatched = s.oneToOneReconciliation(ctx, []*model.Transaction{externalTxn}, matchingRules)
				case "one_to_many":
					batchMatches, batchUnmatched = s.oneToManyReconciliation(ctx, []*model.Transaction{externalTxn}, groupingCriteria, matchingRules)
				case "many_external_to_one_internal":
					batchMatches, batchUnmatched = s.manyToOneReconciliation(ctx, []*model.Transaction{externalTxn}, groupingCriteria, matchingRules)
				default:
					results <- BatchJobResult{Error: fmt.Errorf("unsupported reconciliation strategy: %s", strategy)}
					return
				}

				matches = append(matches, batchMatches...)
				unmatched = append(unmatched, batchUnmatched...)

				if !reconciliation.IsDryRun {
					if err := s.storeMatches(ctx, reconciliation.ReconciliationID, batchMatches); err != nil {
						results <- BatchJobResult{Error: err}
						return
					}
				}

				progress.LastProcessedExternalTxnID = externalTxn.TransactionID
				progress.ProcessedCount++

				if progress.ProcessedCount%100 == 0 { // Save progress every 100 transactions
					if err := s.saveReconciliationProgress(ctx, reconciliation.ReconciliationID, progress); err != nil {
						log.Printf("Error saving reconciliation progress: %v", err)
					}
				}

				results <- BatchJobResult{} // Signal successful processing
			}
		},
	)

	if err != nil {
		return err
	}

	reconciliation.Status = StatusCompleted
	completedAt := time.Now()
	reconciliation.CompletedAt = &completedAt

	if !reconciliation.IsDryRun {
		s.postReconciliationActions(ctx, reconciliation)
	} else {
		// For dry run, just log the results
		log.Printf("Dry run completed. Matches: %d, Unmatched: %d", len(matches), len(unmatched))
		// You might want to store these results somewhere or return them to the caller
	}

	return s.datasource.UpdateReconciliationStatus(ctx, reconciliation.ReconciliationID, StatusCompleted, len(matches), len(unmatched))
}

func (s *Blnk) oneToOneReconciliation(ctx context.Context, externalTxns []*model.Transaction, matchingRules []model.MatchingRule) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	// Create buffered channels for parallel processing
	matchChan := make(chan model.Match, len(externalTxns))
	unmatchedChan := make(chan string, len(externalTxns))

	workerCount := 10
	semaphore := make(chan struct{}, workerCount)

	var wg sync.WaitGroup

	for _, externalTxn := range externalTxns {
		wg.Add(1)
		go func(extTxn *model.Transaction) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			match, err := s.findMatchingInternalTransaction(ctx, extTxn, matchingRules)
			if err != nil {
				unmatchedChan <- extTxn.TransactionID
				log.Printf("No match found for external transaction %s: %v", extTxn.TransactionID, err)
			} else {
				matchChan <- *match
			}
		}(externalTxn)
	}

	// Close channels when all goroutines are done
	go func() {
		wg.Wait()
		close(matchChan)
		close(unmatchedChan)
	}()

	// Collect results
	for match := range matchChan {
		matches = append(matches, match)
	}
	for unmatchedID := range unmatchedChan {
		unmatched = append(unmatched, unmatchedID)
	}

	return matches, unmatched
}

func (s *Blnk) findMatchingInternalTransaction(ctx context.Context, externalTxn *model.Transaction, matchingRules []model.MatchingRule) (*model.Match, error) {
	var match *model.Match
	var matchFound bool

	_, err := s.ProcessTransactionInBatches(
		ctx,
		externalTxn.TransactionID,
		externalTxn.Amount,
		s.getInternalTransactionsPaginated,
		func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
			defer wg.Done()
			for internalTxn := range jobs {
				if s.matchesRules(externalTxn, *internalTxn, matchingRules) {
					match = &model.Match{
						ExternalTransactionID: externalTxn.TransactionID,
						InternalTransactionID: internalTxn.TransactionID,
						Amount:                externalTxn.Amount,
						Date:                  externalTxn.CreatedAt,
					}
					matchFound = true
					results <- BatchJobResult{} // Signal to stop processing
					return
				}
			}
		},
	)

	if err != nil {
		return nil, err
	}

	if !matchFound {
		return nil, fmt.Errorf("no matching internal transaction found")
	}

	return match, nil
}

func (s *Blnk) getExternalTransactionsPaginated(ctx context.Context, uploadID string, limit int, offset int64) ([]*model.Transaction, error) {
	externalTransaction, err := s.datasource.GetExternalTransactionsPaginated(ctx, uploadID, limit, int64(offset))
	if err != nil {
		return nil, err
	}
	transactions := make([]*model.Transaction, len(externalTransaction))

	for i, txn := range externalTransaction {
		transactions[i] = txn.ToInternalTransaction()
	}
	return transactions, nil
}

func (s *Blnk) getInternalTransactionsPaginated(ctx context.Context, id string, limit int, offset int64) ([]*model.Transaction, error) {
	return s.datasource.GetTransactionsPaginated(ctx, "", limit, offset)
}

func (s *Blnk) groupInternalTransactions(ctx context.Context, groupingCriteria map[string]interface{}, batchSize int, offset int64) (map[string][]model.Transaction, error) {
	return s.datasource.GroupTransactions(ctx, groupingCriteria, batchSize, offset)
}

func (s *Blnk) findMatchingGroup(externalTxn *model.Transaction, groupedInternalTxns map[string][]model.Transaction, matchingRules []model.MatchingRule) ([]model.Transaction, error) {
	for _, group := range groupedInternalTxns {
		allRulesMet := true
		for _, rule := range matchingRules {
			ruleMet := false
			for _, internalTxn := range group {
				if s.matchesRules(externalTxn, internalTxn, []model.MatchingRule{rule}) {
					ruleMet = true
					break
				}
			}
			if !ruleMet {
				allRulesMet = false
				break
			}
		}
		if allRulesMet {
			return group, nil
		}
	}
	return nil, fmt.Errorf("no matching group found")
}

func (s *Blnk) oneToManyReconciliation(ctx context.Context, externalTxns []*model.Transaction, groupingCriteria map[string]interface{}, matchingRules []model.MatchingRule) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	batchSize := 1000
	offset := int64(0)

	// Create a map to store processed external transactions
	processedExternalTxns := make(map[string]bool)

	for {
		// Group internal transactions based on the grouping criteria
		groupedInternalTxns, err := s.groupInternalTransactions(ctx, groupingCriteria, batchSize, offset)
		if err != nil {
			log.Printf("Error grouping internal transactions: %v", err)
			return nil, []string{err.Error()}
		}

		if len(groupedInternalTxns) == 0 {
			break // No more transactions to process
		}

		// Process external transactions in parallel
		var wg sync.WaitGroup
		matchChan := make(chan model.Match, len(externalTxns))
		unmatchedChan := make(chan string, len(externalTxns))

		for _, externalTxn := range externalTxns {
			if processedExternalTxns[externalTxn.TransactionID] {
				continue // Skip already processed transactions
			}

			wg.Add(1)
			go func(extTxn *model.Transaction) {
				defer wg.Done()
				matchedGroup, err := s.findMatchingGroup(extTxn, groupedInternalTxns, matchingRules)
				if err != nil {
					unmatchedChan <- extTxn.TransactionID
					return
				}

				totalInternalAmount := 0.0
				for _, internalTxn := range matchedGroup {
					totalInternalAmount += internalTxn.Amount
					matchChan <- model.Match{
						ExternalTransactionID: extTxn.TransactionID,
						InternalTransactionID: internalTxn.TransactionID,
						Amount:                internalTxn.Amount,
						Date:                  internalTxn.CreatedAt,
					}
				}

				// Check if the total amount of internal transactions matches the external transaction
				if math.Abs(extTxn.Amount-totalInternalAmount) > 0.01 {
					unmatchedChan <- extTxn.TransactionID
				}

				processedExternalTxns[extTxn.TransactionID] = true
			}(externalTxn)
		}

		// Wait for all goroutines to finish
		go func() {
			wg.Wait()
			close(matchChan)
			close(unmatchedChan)
		}()

		// Collect results
		for match := range matchChan {
			matches = append(matches, match)
		}
		for unmatchedID := range unmatchedChan {
			unmatched = append(unmatched, unmatchedID)
		}

		offset += int64(batchSize)
	}

	return matches, unmatched
}

func (s *Blnk) manyToOneReconciliation(ctx context.Context, externalTxns []*model.Transaction, groupingCriteria map[string]interface{}, matchingRules []model.MatchingRule) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	internalTxnID, ok := groupingCriteria["internal_transaction_id"].(string)
	if !ok {
		return nil, []string{"Error: invalid internal_transaction_id in grouping criteria"}
	}

	startDate, ok := groupingCriteria["start_date"].(time.Time)
	if !ok {
		return nil, []string{"Error: invalid start_date in grouping criteria"}
	}

	endDate, ok := groupingCriteria["end_date"].(time.Time)
	if !ok {
		return nil, []string{"Error: invalid end_date in grouping criteria"}
	}

	internalTxn, err := s.GetTransaction(internalTxnID)
	if err != nil {
		log.Printf("Error fetching internal transaction: %v", err)
		return nil, []string{fmt.Sprintf("Error fetching internal transaction: %v", err)}
	}

	var totalExternalAmount float64
	var matchedExternalTxns []*model.Transaction

	// Use a worker pool for parallel processing
	numWorkers := runtime.NumCPU()
	jobs := make(chan *model.Transaction, len(externalTxns))
	results := make(chan struct {
		txn       *model.Transaction
		isMatched bool
	}, len(externalTxns))

	// Start worker pool
	for w := 0; w < numWorkers; w++ {
		go manyToOneWorker(ctx, jobs, results, startDate, endDate, internalTxn, matchingRules, s.matchesRules)
	}

	// Send jobs to workers
	for _, txn := range externalTxns {
		jobs <- txn
	}
	close(jobs)

	// Collect results
	for range externalTxns {
		result := <-results
		if result.isMatched {
			totalExternalAmount += result.txn.Amount
			matchedExternalTxns = append(matchedExternalTxns, result.txn)
		} else {
			unmatched = append(unmatched, result.txn.TransactionID)
		}
	}

	// Check if the total amount of matched external transactions equals the internal transaction amount
	allowableDrift := internalTxn.Amount * 0.01 // 1% allowable drift
	if math.Abs(totalExternalAmount-internalTxn.Amount) <= allowableDrift {
		for _, matchedExternalTxn := range matchedExternalTxns {
			matches = append(matches, model.Match{
				ExternalTransactionID: matchedExternalTxn.TransactionID,
				InternalTransactionID: internalTxn.TransactionID,
				Amount:                matchedExternalTxn.Amount,
				Date:                  matchedExternalTxn.CreatedAt,
			})
		}
	} else {
		// If the total doesn't match, consider all external transactions as unmatched
		for _, matchedExternalTxn := range matchedExternalTxns {
			unmatched = append(unmatched, matchedExternalTxn.TransactionID)
		}
		log.Printf("Total amount mismatch: Internal %.2f, External %.2f", internalTxn.Amount, totalExternalAmount)
	}

	return matches, unmatched
}

func manyToOneWorker(_ context.Context, jobs <-chan *model.Transaction, results chan<- struct {
	txn       *model.Transaction
	isMatched bool
}, startDate, endDate time.Time, internalTxn *model.Transaction, matchingRules []model.MatchingRule,
	matchFunc func(*model.Transaction, model.Transaction, []model.MatchingRule) bool) {
	for txn := range jobs {
		isMatched := false
		if (txn.CreatedAt.After(startDate) || txn.CreatedAt.Equal(startDate)) &&
			(txn.CreatedAt.Before(endDate) || txn.CreatedAt.Equal(endDate)) {
			isMatched = matchFunc(txn, *internalTxn, matchingRules)
		}
		results <- struct {
			txn       *model.Transaction
			isMatched bool
		}{txn, isMatched}
	}
}

func (s *Blnk) CreateMatchingRule(ctx context.Context, rule model.MatchingRule) (*model.MatchingRule, error) {
	rule.RuleID = model.GenerateUUIDWithSuffix("rule")
	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()

	err := s.validateRule(&rule)
	if err != nil {
		return nil, err
	}

	err = s.datasource.RecordMatchingRule(ctx, &rule)
	if err != nil {
		return nil, err
	}

	return &rule, nil
}

func (s *Blnk) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	rule, err := s.datasource.GetMatchingRule(ctx, id)
	if err != nil {
		return nil, err
	}
	return rule, nil
}

func (s *Blnk) UpdateMatchingRule(ctx context.Context, rule model.MatchingRule) (*model.MatchingRule, error) {
	existingRule, err := s.GetMatchingRule(ctx, rule.RuleID)
	if err != nil {
		return nil, err
	}

	rule.CreatedAt = existingRule.CreatedAt
	rule.UpdatedAt = time.Now()

	err = s.validateRule(&rule)
	if err != nil {
		return nil, err
	}

	err = s.datasource.UpdateMatchingRule(ctx, &rule)
	if err != nil {
		return nil, err
	}

	return &rule, nil
}

func (s *Blnk) DeleteMatchingRule(ctx context.Context, id string) error {
	return s.datasource.DeleteMatchingRule(ctx, id)
}

func (s *Blnk) ListMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	return s.datasource.GetMatchingRules(ctx)
}

func (s *Blnk) validateRule(rule *model.MatchingRule) error {
	if rule.Name == "" {
		return errors.New("rule name is required")
	}

	if len(rule.Criteria) == 0 {
		return errors.New("at least one matching criteria is required")
	}

	for _, criteria := range rule.Criteria {
		if criteria.Field == "" || criteria.Operator == "" {
			return errors.New("field and operator are required for each criteria")
		}

		// Validate operator
		validOperators := []string{"equals", "greater_than", "less_than", "contains"}
		if !contains(validOperators, criteria.Operator) {
			return errors.New("invalid operator")
		}

		// Validate field
		validFields := []string{"amount", "date", "description", "reference", "currency"}
		if !contains(validFields, criteria.Field) {
			return errors.New("invalid field")
		}

		// Validate Drift
		if criteria.Operator == "equals" {
			if criteria.Field == "amount" {
				if criteria.AllowableDrift < 0 || criteria.AllowableDrift > 100 {
					return errors.New("drift for amount must be between 0 and 100 (percentage)")
				}
			} else if criteria.Field == "date" {
				if criteria.AllowableDrift < 0 {
					return errors.New("drift for date must be non-negative (seconds)")
				}
			}
		}
	}

	return nil
}

func (s *Blnk) getMatchingRules(ctx context.Context, matchingRuleIDs []string) ([]model.MatchingRule, error) {
	var rules []model.MatchingRule
	for _, id := range matchingRuleIDs {
		rule, err := s.GetMatchingRule(ctx, id)
		if err != nil {
			return nil, err
		}
		rules = append(rules, *rule)
	}
	return rules, nil
}

func (s *Blnk) matchesCriteria(externalTxn *model.Transaction, internalTxn model.Transaction, criteria model.MatchingCriteria) bool {
	switch criteria.Field {
	case "amount":
		return s.matchesAmount(externalTxn.Amount, internalTxn.Amount, criteria)
	case "date":
		return s.matchesDate(externalTxn.CreatedAt, internalTxn.CreatedAt, criteria)
	case "description":
		return s.matchesString(externalTxn.Description, internalTxn.Description, criteria)
	case "reference":
		return s.matchesString(externalTxn.Reference, internalTxn.Reference, criteria)
	case "currency":
		return s.matchesString(externalTxn.Currency, internalTxn.Currency, criteria)
	}
	return false
}
func (s *Blnk) matchesString(externalValue, internalValue string, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		return strings.EqualFold(externalValue, internalValue)
	case "contains":
		return s.partialMatch(externalValue, internalValue, criteria.AllowableDrift)
	}
	return false
}

func (s *Blnk) partialMatch(str1, str2 string, allowableDrift float64) bool {
	// Convert strings to lowercase for case-insensitive comparison
	str1 = strings.ToLower(str1)
	str2 = strings.ToLower(str2)

	// Check if either string contains the other
	if strings.Contains(str1, str2) || strings.Contains(str2, str1) {
		return true
	}

	// Calculate Levenshtein distance
	distance := levenshtein.DistanceForStrings([]rune(str1), []rune(str2), levenshtein.DefaultOptions)

	// Calculate the maximum allowed distance based on the length of the longer string and the allowable drift
	maxLength := float64(max(len(str1), len(str2)))
	maxAllowedDistance := int(maxLength * (allowableDrift / 100))

	// Return true if the distance is within the allowed range
	return distance <= maxAllowedDistance
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (s *Blnk) matchesAmount(externalAmount, internalAmount float64, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		allowableDrift := internalAmount * (criteria.AllowableDrift)
		return math.Abs(externalAmount-internalAmount) <= allowableDrift
	case "greater_than":
		return externalAmount > internalAmount
	case "less_than":
		return externalAmount < internalAmount
	}
	return false
}

func (s *Blnk) matchesDate(externalDate, internalDate time.Time, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		difference := externalDate.Sub(internalDate)
		seconds := int64(difference.Seconds()) // Convert to whole seconds
		return math.Abs(float64(seconds)) <= criteria.AllowableDrift
	case "before":
		return externalDate.Before(internalDate)
	case "after":
		return externalDate.After(internalDate)
	}
	return false
}
