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

func (s *Blnk) storeMatches(ctx context.Context, reconciliationID string, matches []model.Match) error {
	for _, match := range matches {
		match.ReconciliationID = reconciliationID
		return s.datasource.RecordMatch(ctx, &match)
	}
	return nil
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

func (s *Blnk) matchesRules(externalTxn *model.Transaction, groupTxn model.Transaction, rules []model.MatchingRule) bool {
	for _, rule := range rules {
		allCriteriaMet := true
		for _, criteria := range rule.Criteria {
			var criterionMet bool
			switch criteria.Field {
			case "amount":
				criterionMet = s.matchesGroupAmount(externalTxn.Amount, groupTxn.Amount, criteria)
			case "date":
				criterionMet = s.matchesGroupDate(externalTxn.CreatedAt, groupTxn.CreatedAt, criteria)
			case "description":
				criterionMet = s.matchesString(externalTxn.Description, groupTxn.Description, criteria)
			case "reference":
				criterionMet = s.matchesString(externalTxn.Reference, groupTxn.Reference, criteria)
			case "currency":
				criterionMet = s.matchesCurrency(externalTxn.Currency, groupTxn.Currency, criteria)
			}
			if !criterionMet {
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
					batchMatches, batchUnmatched = s.groupToNReconciliation(ctx, []*model.Transaction{externalTxn}, groupingCriteria, matchingRules, false)
				case "many_to_one":
					batchMatches, batchUnmatched = s.groupToNReconciliation(ctx, []*model.Transaction{externalTxn}, groupingCriteria, matchingRules, true)
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

func (s *Blnk) groupToNReconciliation(ctx context.Context, externalTxns []*model.Transaction, groupingCriteria map[string]interface{}, matchingRules []model.MatchingRule, isExternalGrouped bool) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	batchSize := 100
	offset := int64(0)

	var groupedTxns map[string][]*model.Transaction
	var singleTxns []*model.Transaction

	if isExternalGrouped {
		groupedTxns = s.groupExternalTransactions(externalTxns)
		// Fetch internal transactions in batches
		for {
			internalTxns, err := s.getInternalTransactionsPaginated(ctx, "", batchSize, offset)
			if err != nil {
				log.Printf("Error fetching internal transactions: %v", err)
				break
			}

			if len(internalTxns) == 0 {
				break
			}
			singleTxns = append(singleTxns, internalTxns...)

			offset += int64(len(internalTxns))
		}
	} else {
		singleTxns = externalTxns
		// Group internal transactions
		var err error
		groupedTxns, err = s.groupInternalTransactions(ctx, groupingCriteria, batchSize, 0)
		if err != nil {
			log.Printf("Error grouping internal transactions: %v", err)
			return nil, []string{}
		}
	}

	for _, singleTxn := range singleTxns {
		matched := false
		for groupKey, groupedTxnList := range groupedTxns {
			if s.matchesGroup(singleTxn, groupedTxnList, matchingRules) {
				for _, groupedTxn := range groupedTxnList {
					var externalID, internalID string
					if isExternalGrouped {
						externalID = groupedTxn.TransactionID
						internalID = singleTxn.TransactionID
					} else {
						externalID = singleTxn.TransactionID
						internalID = groupedTxn.TransactionID
					}
					matches = append(matches, model.Match{
						ExternalTransactionID: externalID,
						InternalTransactionID: internalID,
						Amount:                groupedTxn.Amount,
						Date:                  groupedTxn.CreatedAt,
					})
				}
				matched = true
				delete(groupedTxns, groupKey)
				break
			}
		}
		if !matched {
			unmatched = append(unmatched, singleTxn.TransactionID)
		}
	}

	// Add remaining unmatched grouped transactions
	for _, group := range groupedTxns {
		for _, txn := range group {
			unmatched = append(unmatched, txn.TransactionID)
		}
	}

	return matches, unmatched
}

func (s *Blnk) groupInternalTransactions(ctx context.Context, groupingCriteria map[string]interface{}, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	return s.datasource.GroupTransactions(ctx, groupingCriteria, batchSize, offset)
}

func (s *Blnk) groupExternalTransactions(externalTxns []*model.Transaction) map[string][]*model.Transaction {
	grouped := make(map[string][]*model.Transaction)

	for _, txn := range externalTxns {
		key := fmt.Sprintf("%s_%s", txn.Currency, txn.CreatedAt.Format("2006-01-02"))
		grouped[key] = append(grouped[key], txn)
	}

	return grouped
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

func (s *Blnk) matchesGroup(externalTxn *model.Transaction, group []*model.Transaction, matchingRules []model.MatchingRule) bool {
	var totalAmount float64
	var minDate, maxDate time.Time
	descriptions := make([]string, 0, len(group))
	references := make([]string, 0, len(group))
	currencies := make(map[string]bool)

	for i, internalTxn := range group {
		totalAmount += internalTxn.Amount

		if i == 0 || internalTxn.CreatedAt.Before(minDate) {
			minDate = internalTxn.CreatedAt
		}
		if i == 0 || internalTxn.CreatedAt.After(maxDate) {
			maxDate = internalTxn.CreatedAt
		}

		descriptions = append(descriptions, internalTxn.Description)
		references = append(references, internalTxn.Reference)
		currencies[internalTxn.Currency] = true
	}

	// Create a virtual transaction representing the group
	groupTxn := model.Transaction{
		Amount:      totalAmount,
		CreatedAt:   minDate, // We'll use the earliest date in the group
		Description: strings.Join(descriptions, " | "),
		Reference:   strings.Join(references, " | "),
		Currency:    s.dominantCurrency(currencies),
	}

	// Check if the external transaction matches the group using the matching rules
	return s.matchesRules(externalTxn, groupTxn, matchingRules)
}

// Helper function to determine the dominant currency
func (s *Blnk) dominantCurrency(currencies map[string]bool) string {
	if len(currencies) == 1 {
		for currency := range currencies {
			return currency
		}
	}
	return "MIXED" // Or handle multiple currencies as appropriate for your use case
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

func (s *Blnk) matchesString(externalValue, internalValue string, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		// Split the internal value and check if any part matches exactly
		for _, part := range strings.Split(internalValue, " | ") {
			if strings.EqualFold(externalValue, part) {
				return true
			}
		}
		return false
	case "contains":
		// Check if the external value is contained in any part of the internal value
		for _, part := range strings.Split(internalValue, " | ") {
			if s.partialMatch(externalValue, part, criteria.AllowableDrift) {
				return true
			}
		}
		return false
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

func (s *Blnk) matchesCurrency(externalValue, internalValue string, criteria model.MatchingCriteria) bool {
	if internalValue == "MIXED" {
		// Decide how to handle mixed currencies. Maybe always return true, or implement a more complex logic
		return true
	}
	return s.matchesString(externalValue, internalValue, criteria)
}

func (s *Blnk) matchesGroupAmount(externalAmount, groupAmount float64, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		allowableDrift := groupAmount * criteria.AllowableDrift
		return math.Abs(externalAmount-groupAmount) <= allowableDrift
	case "greater_than":
		return externalAmount > groupAmount
	case "less_than":
		return externalAmount < groupAmount
	}
	return false
}

func (s *Blnk) matchesGroupDate(externalDate, groupEarliestDate time.Time, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		difference := externalDate.Sub(groupEarliestDate)
		return math.Abs(difference.Seconds()) <= criteria.AllowableDrift
	case "after":
		return externalDate.After(groupEarliestDate)
	case "before":
		return externalDate.Before(groupEarliestDate)
	}
	return false
}
