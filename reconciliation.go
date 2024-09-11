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

package blnk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/texttheater/golang-levenshtein/levenshtein"
	"github.com/wacul/ptr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Status constants representing the various states a process can be in.
const (
	StatusStarted    = "started"     // Indicates the process has started.
	StatusInProgress = "in_progress" // Indicates the process is ongoing.
	StatusCompleted  = "completed"   // Indicates the process is finished successfully.
	StatusFailed     = "failed"      // Indicates the process has failed.
)

// transactionProcessor represents the processor for handling reconciliation-related transactions.
// Fields:
// - reconciliation: The reconciliation object that holds transaction data to be processed.
// - progress: Tracks the progress of the reconciliation process.
// - reconciler: A function that handles the reconciliation logic for a batch of transactions.
// - matches: Counter for transactions that have been successfully matched.
// - unmatched: Counter for transactions that couldn't be matched.
// - datasource: The interface for database operations, enabling interaction with the data source.
// - progressSaveCount: The number of transactions processed before saving progress.
type transactionProcessor struct {
	reconciliation    model.Reconciliation
	progress          model.ReconciliationProgress
	reconciler        reconciler
	matches           int
	unmatched         int
	datasource        database.IDataSource
	progressSaveCount int
}

// reconciler defines the function type for reconciling a batch of transactions.
// It accepts a context, and a slice of transactions, and returns the matched transactions and unmatched ones.
type reconciler func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string)

// detectFileType attempts to detect the file type based on its extension or content.
// If the file extension can identify the type, it returns that, otherwise, it inspects the content of the file.
// Parameters:
// - data: The file content as a byte slice.
// - filename: The name of the file to detect by extension.
// Returns:
// - string: The detected file type (MIME type).
// - error: If content detection fails.
func detectFileType(data []byte, filename string) (string, error) {
	// Attempt to detect file type by its extension first.
	if mimeType := detectByExtension(filename); mimeType != "" {
		return mimeType, nil
	}
	// If detection by extension fails, analyze the content.
	return detectByContent(data)
}

// detectByExtension detects the MIME type by the file extension.
// Parameters:
// - filename: The name of the file to detect by extension.
// Returns:
// - string: The MIME type corresponding to the file extension.
func detectByExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename)) // Extract and lower the file extension.
	return mime.TypeByExtension(ext)               // Use the standard library to get MIME type.
}

// detectByContent detects the MIME type based on the content of the file.
// Parameters:
// - data: The file content as a byte slice.
// Returns:
// - string: The detected MIME type.
// - error: If content analysis fails.
func detectByContent(data []byte) (string, error) {
	mimeType := http.DetectContentType(data) // Detect content type by analyzing the first 512 bytes.

	switch mimeType {
	case "application/octet-stream", "text/plain":
		// If detected as binary or plain text, analyze the content further.
		return analyzeTextContent(data)
	case "text/csv; charset=utf-8":
		// Directly return if CSV is detected.
		return "text/csv", nil
	default:
		return mimeType, nil // Return detected MIME type.
	}
}

// analyzeTextContent further inspects text-based content to differentiate between CSV, JSON, or plain text.
// Parameters:
// - data: The file content as a byte slice.
// Returns:
// - string: The detected MIME type (either CSV, JSON, or plain text).
// - error: If content analysis fails.
func analyzeTextContent(data []byte) (string, error) {
	if looksLikeCSV(data) {
		return "text/csv", nil
	}
	if json.Valid(data) {
		return "application/json", nil
	}
	return "text/plain", nil // Default to plain text if no other format matches.
}

// looksLikeCSV checks whether the provided data looks like a CSV file.
// It checks for multiple lines and ensures they have the same number of fields (based on commas).
// Parameters:
// - data: The file content as a byte slice.
// Returns:
// - bool: True if the content seems like a CSV file, otherwise false.
func looksLikeCSV(data []byte) bool {
	lines := bytes.Split(data, []byte("\n")) // Split the content into lines.
	if len(lines) < 2 {
		return false // Require at least two lines for CSV.
	}

	// Count the number of fields (columns) in the first line.
	fields := bytes.Count(lines[0], []byte(",")) + 1
	// Ensure all subsequent lines have the same number of fields.
	for _, line := range lines[1:] {
		if len(line) == 0 {
			continue // Skip empty lines.
		}
		if bytes.Count(line, []byte(","))+1 != fields {
			return false // Return false if field count doesn't match.
		}
	}

	return fields > 1 // Return true if there are at least two fields.
}

// parseFloat parses a string into a float64 value.
// If the string is not a valid float, it returns 0.
// Parameters:
// - s: The string to parse.
// Returns:
// - float64: The parsed float value or 0 if parsing fails.
func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0 // Return 0 if parsing fails.
	}
	return f
}

// parseTime parses a string into a time.Time object in RFC3339 format.
// If parsing fails, it returns a zero-value time.Time object.
// Parameters:
// - s: The string to parse into a timestamp.
// Returns:
// - time.Time: The parsed time object or zero time if parsing fails.
func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{} // Return zero time if parsing fails.
	}
	return t
}

// contains checks whether a slice contains a specific string.
// Parameters:
// - slice: The slice to check.
// - item: The item to look for in the slice.
// Returns:
// - bool: True if the item is found in the slice, otherwise false.
func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

// parseAndStoreCSV reads and processes a CSV file from an io.Reader, parsing each row and storing the corresponding transactions.
// Parameters:
// - ctx: The context for controlling execution.
// - uploadID: The unique ID of the current upload.
// - source: The source of the external data.
// - reader: An io.Reader for reading the CSV data.
// Returns:
// - error: If parsing or storing fails.
func (s *Blnk) parseAndStoreCSV(ctx context.Context, uploadID, source string, reader io.Reader) error {
	csvReader := csv.NewReader(bufio.NewReader(reader))

	// Read the header row to determine column mapping.
	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV headers: %w", err)
	}

	// Create a column map to associate column names with their indices.
	columnMap, err := createColumnMap(headers)
	if err != nil {
		return err
	}

	// Process the CSV rows based on the column map.
	return s.processCSVRows(ctx, uploadID, source, csvReader, columnMap)
}

// processCSVRows reads and processes each row in the CSV file, parsing the fields and storing the transactions.
// Parameters:
// - ctx: The context for controlling execution.
// - uploadID: The unique ID of the current upload.
// - source: The source of the external data.
// - csvReader: The CSV reader for reading rows.
// - columnMap: The map associating column names with their indices.
// Returns:
// - error: If parsing or storing any row fails.
func (s *Blnk) processCSVRows(ctx context.Context, uploadID, source string, csvReader *csv.Reader, columnMap map[string]int) error {
	var errs []error // To accumulate any errors encountered during processing.
	rowNum := 1      // Row number starts at 1 to account for the header row.

	for {
		record, err := csvReader.Read() // Read the next row.
		if err == io.EOF {
			break // Stop processing if end of file is reached.
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("error reading row %d: %w", rowNum, err))
			continue // Continue processing other rows even if this row fails.
		}

		rowNum++ // Increment row number.

		// Parse the row into an ExternalTransaction object.
		externalTxn, err := parseExternalTransaction(record, columnMap, source)
		if err != nil {
			errs = append(errs, fmt.Errorf("error parsing row %d: %w", rowNum, err))
			continue // Skip this row if parsing fails.
		}

		// Store the parsed transaction.
		if err := s.storeExternalTransaction(ctx, uploadID, externalTxn); err != nil {
			errs = append(errs, fmt.Errorf("error storing transaction from row %d: %w", rowNum, err))
		}

		// Check for context cancellation every 1000 rows.
		if rowNum%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err() // Return if the context is cancelled.
			default:
			}
		}
	}

	if len(errs) > 0 {
		// If there were errors, return a summary of them.
		return fmt.Errorf("encountered %d errors while processing CSV: %v", len(errs), errs)
	}

	return nil
}

// createColumnMap creates a map of column names to their indices based on the headers row of a CSV file.
// Ensures that required columns (ID, Amount, Date) are present.
// Parameters:
// - headers: The slice of column headers from the CSV file.
// Returns:
// - map[string]int: A map of column names to their respective indices.
// - error: If any required column is missing.
func createColumnMap(headers []string) (map[string]int, error) {
	requiredColumns := []string{"ID", "Amount", "Date"} // Columns that must be present in the CSV.
	columnMap := make(map[string]int)

	// Map each column name to its index.
	for i, header := range headers {
		columnMap[strings.ToLower(strings.TrimSpace(header))] = i
	}

	// Ensure all required columns are present.
	for _, col := range requiredColumns {
		if _, exists := columnMap[strings.ToLower(col)]; !exists {
			return nil, fmt.Errorf("required column '%s' not found in CSV", col)
		}
	}

	return columnMap, nil
}

// parseExternalTransaction parses a row of the CSV file into an ExternalTransaction object.
// Parameters:
// - record: The slice of strings representing the row's fields.
// - columnMap: A map of column names to their indices.
// - source: The source of the external data.
// Returns:
// - model.ExternalTransaction: The parsed ExternalTransaction object.
// - error: If parsing fails due to missing or invalid data.
func parseExternalTransaction(record []string, columnMap map[string]int, source string) (model.ExternalTransaction, error) {
	if len(record) != len(columnMap) {
		// Return an error if the number of fields in the row doesn't match the column map.
		return model.ExternalTransaction{}, fmt.Errorf("incorrect number of fields in record")
	}

	// Get required fields from the record and parse them into appropriate types.
	id, err := getRequiredField(record, columnMap, "id")
	if err != nil {
		return model.ExternalTransaction{}, err
	}

	amountStr, err := getRequiredField(record, columnMap, "amount")
	if err != nil {
		return model.ExternalTransaction{}, err
	}

	currency, err := getRequiredField(record, columnMap, "currency")
	if err != nil {
		return model.ExternalTransaction{}, err
	}

	amount := parseFloat(amountStr)
	if err != nil {
		return model.ExternalTransaction{}, fmt.Errorf("invalid amount: %w", err)
	}

	reference, err := getRequiredField(record, columnMap, "reference")
	if err != nil {
		return model.ExternalTransaction{}, err
	}

	description, err := getRequiredField(record, columnMap, "description")
	if err != nil {
		return model.ExternalTransaction{}, err
	}

	dateStr, err := getRequiredField(record, columnMap, "date")
	if err != nil {
		return model.ExternalTransaction{}, err
	}
	date := parseTime(dateStr)
	if err != nil {
		return model.ExternalTransaction{}, fmt.Errorf("invalid date: %w", err)
	}

	// Return the parsed ExternalTransaction object.
	return model.ExternalTransaction{
		ID:          id,
		Amount:      amount,
		Currency:    currency,
		Reference:   reference,
		Description: description,
		Date:        date,
		Source:      source,
	}, nil
}

// getRequiredField retrieves a field from a CSV record, ensuring it is not empty.
// Parameters:
// - record: The slice of strings representing the row's fields.
// - columnMap: A map of column names to their indices.
// - field: The name of the field to retrieve.
// Returns:
// - string: The value of the field.
// - error: If the field is missing or empty.
func getRequiredField(record []string, columnMap map[string]int, field string) (string, error) {
	if index, exists := columnMap[field]; exists && index < len(record) {
		value := strings.TrimSpace(record[index])
		if value == "" {
			return "", fmt.Errorf("required field '%s' is empty", field)
		}
		return value, nil
	}
	return "", fmt.Errorf("required field '%s' not found in record", field)
}

// parseAndStoreJSON parses and stores transactions from a JSON file.
// Parameters:
// - ctx: The context for controlling execution.
// - uploadID: The unique ID of the current upload.
// - source: The source of the external data.
// - reader: An io.Reader for reading the JSON data.
// Returns:
// - int: The number of parsed transactions.
// - error: If parsing or storing fails.
func (s *Blnk) parseAndStoreJSON(ctx context.Context, uploadID, source string, reader io.Reader) (int, error) {
	decoder := json.NewDecoder(reader)
	var transactions []model.ExternalTransaction
	// Decode the JSON data into a slice of ExternalTransaction objects.
	if err := decoder.Decode(&transactions); err != nil {
		return 0, err
	}

	// Store each parsed transaction.
	for _, txn := range transactions {
		txn.Source = source
		if err := s.storeExternalTransaction(ctx, uploadID, txn); err != nil {
			return 0, err
		}
	}

	return len(transactions), nil
}

// UploadExternalData handles the process of uploading external data by detecting file type, parsing, and storing it.
// Parameters:
// - ctx: The context for controlling execution.
// - source: The source of the external data.
// - reader: An io.Reader for reading the data.
// - filename: The name of the file being uploaded.
// Returns:
// - string: The ID of the upload.
// - int: The total number of records processed.
// - error: If any step of the process fails.
func (s *Blnk) UploadExternalData(ctx context.Context, source string, reader io.Reader, filename string) (string, int, error) {
	uploadID := model.GenerateUUIDWithSuffix("upload") // Generate a unique ID for the upload.

	// Create a temporary file and populate it with the uploaded data.
	tempFile, err := s.createAndPopulateTempFile(filename, reader)
	if err != nil {
		return "", 0, err
	}
	defer s.cleanupTempFile(tempFile) // Ensure the temp file is cleaned up after processing.

	// Detect the file type (CSV or JSON) based on the content.
	fileType, err := s.detectFileTypeFromTempFile(tempFile, filename)
	if err != nil {
		return "", 0, err
	}

	// Parse and store the data based on its file type.
	total, err := s.parseAndStoreData(ctx, uploadID, source, tempFile, fileType)
	if err != nil {
		return "", 0, err
	}

	return uploadID, total, nil
}

// createAndPopulateTempFile creates a temporary file and writes the uploaded data to it.
// Parameters:
// - filename: The original filename of the upload.
// - reader: An io.Reader for reading the upload data.
// Returns:
// - *os.File: The created temporary file.
// - error: If file creation or data writing fails.
func (s *Blnk) createAndPopulateTempFile(filename string, reader io.Reader) (*os.File, error) {
	// Create a new temporary file with a name based on the original filename.
	tempFile, err := s.createTempFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error creating temporary file: %w", err)
	}

	// Copy the uploaded data into the temporary file.
	if _, err := io.Copy(tempFile, reader); err != nil {
		return nil, fmt.Errorf("error copying upload data: %w", err)
	}

	// Reset the file pointer to the beginning for subsequent reading.
	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking temporary file: %w", err)
	}

	return tempFile, nil
}

// detectFileTypeFromTempFile detects the file type by reading the first 512 bytes from the temporary file.
// Parameters:
// - tempFile: The temporary file containing the uploaded data.
// - filename: The original filename of the upload.
// Returns:
// - string: The detected file type (MIME type).
// - error: If file type detection fails.
func (s *Blnk) detectFileTypeFromTempFile(tempFile *os.File, filename string) (string, error) {
	header := make([]byte, 512)
	// Read the first 512 bytes of the file (enough for MIME type detection).
	if _, err := tempFile.Read(header); err != nil && err != io.EOF {
		return "", fmt.Errorf("error reading file header: %w", err)
	}

	// Detect the file type based on the content.
	fileType, err := detectFileType(header, filename)
	if err != nil {
		return "", fmt.Errorf("error detecting file type: %w", err)
	}

	// Reset the file pointer to the beginning for subsequent reading.
	if _, err := tempFile.Seek(0, 0); err != nil {
		return "", fmt.Errorf("error seeking temporary file: %w", err)
	}

	return fileType, nil
}

// parseAndStoreData parses and stores data based on the file type (either CSV or JSON).
// Parameters:
// - ctx: The context for controlling execution.
// - uploadID: The unique ID of the current upload.
// - source: The source of the external data.
// - reader: An io.Reader for reading the data.
// - fileType: The detected MIME type of the file.
// Returns:
// - int: The total number of records processed.
// - error: If parsing or storing fails.
func (s *Blnk) parseAndStoreData(ctx context.Context, uploadID, source string, reader io.Reader, fileType string) (int, error) {
	switch fileType {
	case "text/csv", "text/csv; charset=utf-8":
		// Handle CSV files.
		err := s.parseAndStoreCSV(ctx, uploadID, source, reader)
		return 0, err // CSV parsing doesn't return a count.
	case "application/json":
		// Handle JSON files.
		return s.parseAndStoreJSON(ctx, uploadID, source, reader)
	default:
		// Return an error if the file type is unsupported.
		return 0, fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// createTempFile creates a new temporary file for storing the uploaded data.
// Parameters:
// - originalFilename: The original filename of the upload.
// Returns:
// - *os.File: The created temporary file.
// - error: If the temporary file cannot be created.
func (s *Blnk) createTempFile(originalFilename string) (*os.File, error) {
	// Create the directory for temporary files if it doesn't exist.
	tempDir := filepath.Join(os.TempDir(), "blnk_uploads")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating temporary directory: %w", err)
	}

	// Create a temporary file with a prefix based on the original filename.
	prefix := fmt.Sprintf("%s_", filepath.Base(originalFilename))
	tempFile, err := os.CreateTemp(tempDir, prefix)
	if err != nil {
		return nil, fmt.Errorf("error creating temporary file: %w", err)
	}

	return tempFile, nil
}

// cleanupTempFile removes the specified temporary file from the filesystem.
// Parameters:
// - file: The temporary file to remove.
func (s *Blnk) cleanupTempFile(file *os.File) {
	if file != nil {
		filename := file.Name() // Get the file name.
		file.Close()            // Close the file before removing it.
		if err := os.Remove(filename); err != nil {
			// Log any errors encountered during file removal.
			log.Printf("Error removing temporary file %s: %v", filename, err)
		}
	}
}

// storeExternalTransaction stores an external transaction in the datasource.
// Parameters:
// - ctx: The context for controlling execution.
// - uploadID: The unique ID of the current upload.
// - txn: The external transaction to store.
// Returns:
// - error: If storing the transaction fails.
func (s *Blnk) storeExternalTransaction(ctx context.Context, uploadID string, txn model.ExternalTransaction) error {
	return s.datasource.RecordExternalTransaction(ctx, &txn, uploadID)
}

// postReconciliationActions queues the indexing of reconciliation data in the background.
// This allows the reconciliation to be indexed without blocking the main process.
// Parameters:
// - ctx: The context for controlling execution.
// - reconciliation: The reconciliation object to be indexed.
func (l *Blnk) postReconciliationActions(_ context.Context, reconciliation model.Reconciliation) {
	go func() {
		// Queue the reconciliation data for indexing.
		err := l.queue.queueIndexData(reconciliation.ReconciliationID, "reconciliations", reconciliation)
		if err != nil {
			// If there is an error, notify through the notification system.
			notification.NotifyError(err)
		}
	}()
}

// StartReconciliation initiates the reconciliation process by creating a new reconciliation entry and starting the
// process asynchronously. The process is detached to run in the background.
// Parameters:
// - ctx: The context controlling the reconciliation process.
// - uploadID: The ID of the uploaded transaction file to reconcile.
// - strategy: The reconciliation strategy to be used (e.g., "one_to_one").
// - groupCriteria: Criteria to group transactions (optional).
// - matchingRuleIDs: The IDs of the rules used for matching transactions.
// - isDryRun: If true, the reconciliation will not commit changes (useful for testing).
// Returns:
// - string: The ID of the reconciliation process.
// - error: If the reconciliation fails to start.
func (s *Blnk) StartReconciliation(ctx context.Context, uploadID string, strategy string, groupCriteria string, matchingRuleIDs []string, isDryRun bool) (string, error) {
	// Generate a unique ID for the reconciliation.
	reconciliationID := model.GenerateUUIDWithSuffix("recon")
	// Initialize a new reconciliation object with the provided parameters.
	reconciliation := model.Reconciliation{
		ReconciliationID: reconciliationID,
		UploadID:         uploadID,
		Status:           StatusStarted,
		StartedAt:        time.Now(),
		IsDryRun:         isDryRun,
	}

	// Record the reconciliation in the data source (e.g., database).
	if err := s.datasource.RecordReconciliation(ctx, &reconciliation); err != nil {
		return "", err
	}

	// Detach the context to allow the reconciliation process to run in the background.
	detachedCtx := context.Background()
	ctxWithTrace := trace.ContextWithSpan(detachedCtx, trace.SpanFromContext(ctx))

	// Start the reconciliation process asynchronously.
	go func() {
		err := s.processReconciliation(ctxWithTrace, reconciliation, strategy, groupCriteria, matchingRuleIDs)
		if err != nil {
			// If an error occurs during the reconciliation, log it and update the reconciliation status to "failed".
			log.Printf("Error in reconciliation process: %v", err)
			err := s.datasource.UpdateReconciliationStatus(ctxWithTrace, reconciliationID, StatusFailed, 0, 0)
			if err != nil {
				log.Printf("Error updating reconciliation status: %v", err)
			}
		}
	}()

	return reconciliationID, nil
}

// matchesRules checks whether an external transaction matches a group transaction based on specified matching rules.
// It iterates through the rules and criteria, evaluating whether the transactions meet the conditions for a match.
// Parameters:
// - externalTxn: The external transaction to match.
// - groupTxn: The internal or group transaction to compare against.
// - rules: A list of matching rules containing criteria to apply.
// Returns:
// - bool: True if the transactions match based on the rules, otherwise false.
func (s *Blnk) matchesRules(externalTxn *model.Transaction, groupTxn model.Transaction, rules []model.MatchingRule) bool {
	for _, rule := range rules {
		allCriteriaMet := true
		// Iterate through each rule's criteria to check if all conditions are satisfied.
		for _, criteria := range rule.Criteria {
			var criterionMet bool
			// Check each field specified in the criteria.
			switch criteria.Field {
			case "amount":
				// Compare amounts between the external and group transactions.
				criterionMet = s.matchesGroupAmount(externalTxn.Amount, groupTxn.Amount, criteria)
			case "date":
				// Compare the dates of the transactions.
				criterionMet = s.matchesGroupDate(externalTxn.CreatedAt, groupTxn.CreatedAt, criteria)
			case "description":
				// Compare the description fields for a match.
				criterionMet = s.matchesString(externalTxn.Description, groupTxn.Description, criteria)
			case "reference":
				// Compare the transaction references for a match.
				criterionMet = s.matchesString(externalTxn.Reference, groupTxn.Reference, criteria)
			case "currency":
				// Compare the currencies of the transactions.
				criterionMet = s.matchesCurrency(externalTxn.Currency, groupTxn.Currency, criteria)
			}
			// If any criterion is not met, mark the rule as not satisfied.
			if !criterionMet {
				allCriteriaMet = false
				break
			}
		}
		// If all criteria are satisfied, return true (the transactions match).
		if allCriteriaMet {
			return true
		}
	}
	return false
}

// loadReconciliationProgress retrieves the progress of a reconciliation process.
// Parameters:
// - ctx: The context controlling the request.
// - reconciliationID: The ID of the reconciliation to load progress for.
// Returns:
// - model.ReconciliationProgress: The current progress of the reconciliation.
// - error: If the progress cannot be retrieved.
func (s *Blnk) loadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	return s.datasource.LoadReconciliationProgress(ctx, reconciliationID)
}

// processReconciliation manages the full reconciliation process by executing each step: updating the status,
// fetching rules, processing transactions, and finalizing the reconciliation.
// Parameters:
// - ctx: The context controlling the process.
// - reconciliation: The reconciliation object representing the current reconciliation.
// - strategy: The reconciliation strategy (e.g., one-to-one, one-to-many).
// - groupCriteria: Criteria for grouping transactions (optional).
// - matchingRuleIDs: A list of matching rule IDs to apply during the process.
// Returns:
// - error: If any step in the reconciliation process fails.
func (s *Blnk) processReconciliation(ctx context.Context, reconciliation model.Reconciliation, strategy string, groupCriteria string, matchingRuleIDs []string) error {
	// Update the reconciliation status to "in progress".
	if err := s.updateReconciliationStatus(ctx, reconciliation.ReconciliationID, StatusInProgress); err != nil {
		return fmt.Errorf("failed to update reconciliation status: %w", err)
	}

	// Retrieve the matching rules that apply to the reconciliation.
	matchingRules, err := s.getMatchingRules(ctx, matchingRuleIDs)
	if err != nil {
		return fmt.Errorf("failed to get matching rules: %w", err)
	}

	// Initialize the reconciliation progress.
	progress, err := s.initializeReconciliationProgress(ctx, reconciliation.ReconciliationID)
	if err != nil {
		return fmt.Errorf("failed to initialize reconciliation progress: %w", err)
	}

	// Create the reconciler function based on the strategy and rules.
	reconciler := s.createReconciler(strategy, groupCriteria, matchingRules)

	// Create a transaction processor to handle the reconciliation logic.
	processor := s.createTransactionProcessor(reconciliation, progress, reconciler)

	// Process the transactions for reconciliation based on the chosen strategy.
	err = s.processTransactions(ctx, reconciliation.UploadID, processor, strategy)
	if err != nil {
		return fmt.Errorf("failed to process transactions: %w", err)
	}

	// After processing, retrieve the results (matched and unmatched counts).
	matched, unmatched := processor.getResults()

	// Finalize the reconciliation by updating the status and recording the results.
	return s.finalizeReconciliation(ctx, reconciliation, matched, unmatched)
}

// updateReconciliationStatus updates the status of a reconciliation process in the database.
// Parameters:
// - ctx: The context controlling the request.
// - reconciliationID: The ID of the reconciliation.
// - status: The new status to set.
// Returns:
// - error: If the status update fails.
func (s *Blnk) updateReconciliationStatus(ctx context.Context, reconciliationID, status string) error {
	return s.datasource.UpdateReconciliationStatus(ctx, reconciliationID, status, 0, 0)
}

// initializeReconciliationProgress initializes or retrieves the progress of a reconciliation.
// If no progress exists, it creates a new progress entry.
// Parameters:
// - ctx: The context controlling the request.
// - reconciliationID: The ID of the reconciliation.
// Returns:
// - model.ReconciliationProgress: The current or newly initialized progress.
// - error: If the initialization or retrieval fails.
func (s *Blnk) initializeReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	progress, err := s.loadReconciliationProgress(ctx, reconciliationID)
	if err != nil {
		log.Printf("Error loading reconciliation progress: %v", err)
		return model.ReconciliationProgress{}, nil
	}
	return progress, nil
}

// createReconciler creates a reconciler function based on the specified strategy, group criteria, and matching rules.
// Parameters:
// - strategy: The reconciliation strategy (e.g., one-to-one, one-to-many).
// - groupCriteria: Criteria for grouping transactions (optional).
// - matchingRules: A list of matching rules to apply during reconciliation.
// Returns:
// - reconciler: A function that performs reconciliation according to the specified strategy.
func (s *Blnk) createReconciler(strategy string, groupCriteria string, matchingRules []model.MatchingRule) reconciler {
	return func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string) {
		switch strategy {
		case "one_to_one":
			// Perform one-to-one reconciliation.
			return s.oneToOneReconciliation(ctx, txns, matchingRules)
		case "one_to_many":
			// Perform one-to-many reconciliation.
			return s.oneToManyReconciliation(ctx, txns, groupCriteria, matchingRules, false)
		case "many_to_one":
			// Perform many-to-one reconciliation.
			return s.manyToOneReconciliation(ctx, txns, groupCriteria, matchingRules, true)
		default:
			// Log unsupported strategies.
			log.Printf("Unsupported reconciliation strategy: %s", strategy)
			return nil, nil
		}
	}
}

// createTransactionProcessor creates a new transaction processor for the reconciliation.
// Parameters:
// - reconciliation: The reconciliation object representing the current process.
// - progress: The current progress of the reconciliation.
// - reconciler: The reconciler function to apply.
// Returns:
// - *transactionProcessor: The created transaction processor.
func (s *Blnk) createTransactionProcessor(reconciliation model.Reconciliation, progress model.ReconciliationProgress, reconciler func(ctx context.Context, txns []*model.Transaction) ([]model.Match, []string)) *transactionProcessor {
	return &transactionProcessor{
		reconciliation:    reconciliation,
		progress:          progress,
		reconciler:        reconciler,
		datasource:        s.datasource,
		progressSaveCount: 100, // Save progress every 100 transactions.
	}
}

// process handles individual transaction processing, applying the reconciliation logic and recording results.
// Parameters:
// - ctx: The context controlling the request.
// - txn: The transaction to process.
// Returns:
// - error: If processing or recording results fails.
func (tp *transactionProcessor) process(ctx context.Context, txn *model.Transaction) error {
	// Reconcile the batch of transactions and get matches and unmatched transactions.
	batchMatches, batchUnmatched := tp.reconciler(ctx, []*model.Transaction{txn})

	// Increment the counters for matched and unmatched transactions.
	tp.matches += len(batchMatches)
	tp.unmatched += len(batchUnmatched)

	// If the reconciliation is not a dry run, record the matches and unmatched transactions.
	if !tp.reconciliation.IsDryRun {
		if len(batchMatches) > 0 {
			// Record the matched transactions.
			if err := tp.datasource.RecordMatches(ctx, tp.reconciliation.ReconciliationID, batchMatches); err != nil {
				return err
			}
		}

		if len(batchUnmatched) > 0 {
			// Record the unmatched transactions.
			if err := tp.datasource.RecordUnmatched(ctx, tp.reconciliation.ReconciliationID, batchUnmatched); err != nil {
				return err
			}
		}
	}

	// Update the progress with the last processed transaction ID and increment the processed count.
	tp.progress.LastProcessedExternalTxnID = txn.TransactionID
	tp.progress.ProcessedCount++

	// Periodically save the reconciliation progress.
	if tp.progress.ProcessedCount%tp.progressSaveCount == 0 {
		if err := tp.datasource.SaveReconciliationProgress(ctx, tp.reconciliation.ReconciliationID, tp.progress); err != nil {
			log.Printf("Error saving reconciliation progress: %v", err)
		}
	}

	return nil
}

// getResults returns the total counts of matched and unmatched transactions processed during reconciliation.
// Returns:
// - int: The count of matched transactions.
// - int: The count of unmatched transactions.
func (tp *transactionProcessor) getResults() (int, int) {
	return tp.matches, tp.unmatched
}

// processTransactions processes the transactions in batches, applying the reconciliation logic for each transaction.
// Parameters:
// - ctx: The context controlling the request.
// - uploadID: The ID of the uploaded transaction file to reconcile.
// - processor: The transaction processor handling the reconciliation logic.
// - strategy: The reconciliation strategy to apply.
// Returns:
// - error: If any error occurs during processing.
func (s *Blnk) processTransactions(ctx context.Context, uploadID string, processor *transactionProcessor, strategy string) error {
	processedCount := 0
	var transactionProcessor getTxns
	// Use different transaction retrieval methods depending on the strategy.
	if strategy == "many_to_one" {
		transactionProcessor = s.getInternalTransactionsPaginated
	} else {
		transactionProcessor = s.getExternalTransactionsPaginated
	}

	// Process the transactions in batches.
	_, err := s.ProcessTransactionInBatches(
		ctx,
		uploadID,
		0,     // Offset for pagination.
		10,    // Batch size.
		false, // Stream mode is disabled.
		transactionProcessor,
		func(ctx context.Context, txns <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, _ float64) {
			defer wg.Done()
			for txn := range txns {
				// Process each transaction.
				if err := processor.process(ctx, txn); err != nil {
					log.Printf("Error processing transaction %s: %v", txn.TransactionID, err)
					results <- BatchJobResult{Error: err}
					return
				}
				processedCount++
				// Log progress every 10 transactions.
				if processedCount%10 == 0 {
					log.Printf("Processed %d transactions", processedCount)
				}
				results <- BatchJobResult{}
			}
		},
	)
	log.Printf("Total transactions processed: %d", processedCount)
	return err
}

// finalizeReconciliation finalizes the reconciliation process by updating its status and recording the final match/unmatch counts.
// Parameters:
// - ctx: The context controlling the request.
// - reconciliation: The reconciliation object representing the current process.
// - matchCount: The number of matched transactions.
// - unmatchedCount: The number of unmatched transactions.
// Returns:
// - error: If any error occurs during finalization.
func (s *Blnk) finalizeReconciliation(ctx context.Context, reconciliation model.Reconciliation, matchCount, unmatchedCount int) error {
	// Update the reconciliation status to "completed".
	reconciliation.Status = StatusCompleted
	reconciliation.UnmatchedTransactions = unmatchedCount
	reconciliation.MatchedTransactions = matchCount
	reconciliation.CompletedAt = ptr.Time(time.Now())

	log.Printf("Finalizing reconciliation. Matches: %d, Unmatched: %d", matchCount, unmatchedCount)

	// If not a dry run, perform post-reconciliation actions (e.g., indexing).
	if !reconciliation.IsDryRun {
		s.postReconciliationActions(ctx, reconciliation)
	} else {
		// Log the results of the dry run.
		log.Printf("Dry run completed. Matches: %d, Unmatched: %d", matchCount, unmatchedCount)
	}

	// Update the final reconciliation status and counts in the data source.
	err := s.datasource.UpdateReconciliationStatus(ctx, reconciliation.ReconciliationID, StatusCompleted, matchCount, unmatchedCount)
	if err != nil {
		log.Printf("Error updating reconciliation status: %v", err)
		return err
	}

	log.Printf("Reconciliation %s completed. Total matches: %d, Total unmatched: %d", reconciliation.ReconciliationID, matchCount, unmatchedCount)

	return nil
}

// oneToOneReconciliation performs a one-to-one reconciliation, where each external transaction is matched against
// a single internal transaction. The process is parallelized using goroutines.
// Parameters:
// - ctx: Context for managing cancellation and timeouts.
// - externalTxns: The list of external transactions to be reconciled.
// - matchingRules: The rules used to match external transactions to internal transactions.
// Returns:
// - []model.Match: A list of matched transactions.
// - []string: A list of unmatched transaction IDs.
func (s *Blnk) oneToOneReconciliation(ctx context.Context, externalTxns []*model.Transaction, matchingRules []model.MatchingRule) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	matchChan := make(chan model.Match, len(externalTxns)) // Channel to collect matched transactions.
	unmatchedChan := make(chan string, len(externalTxns))  // Channel to collect unmatched transactions.
	var wg sync.WaitGroup                                  // WaitGroup to manage concurrent goroutines.

	// Iterate over each external transaction and attempt to match it against internal transactions.
	for _, externalTxn := range externalTxns {
		wg.Add(1)
		go func(extTxn *model.Transaction) {
			defer wg.Done()
			err := s.findMatchingInternalTransaction(ctx, extTxn, matchingRules, matchChan, unmatchedChan)
			if err != nil {
				unmatchedChan <- extTxn.TransactionID
				log.Printf("No match found for external transaction %s: %v", extTxn.TransactionID, err)
			}
		}(externalTxn)
	}

	// Close the channels after all goroutines are finished.
	go func() {
		wg.Wait()
		close(matchChan)
		close(unmatchedChan)
	}()

	// Collect matches and unmatched transactions from channels.
	for match := range matchChan {
		matches = append(matches, match)
	}
	for unmatchedID := range unmatchedChan {
		unmatched = append(unmatched, unmatchedID)
	}

	return matches, unmatched
}

// oneToManyReconciliation performs a one-to-many reconciliation, where each external transaction can match
// multiple internal transactions grouped by specific criteria.
// Parameters are the same as `oneToOneReconciliation`, with additional support for grouping criteria and a flag
// to determine if the external transactions are grouped.
// Returns:
// - []model.Match: A list of matched transactions.
// - []string: A list of unmatched transaction IDs.
func (s *Blnk) oneToManyReconciliation(ctx context.Context, externalTxns []*model.Transaction, groupCriteria string, matchingRules []model.MatchingRule, isExternalGrouped bool) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	matchChan := make(chan model.Match, len(externalTxns))
	unmatchedChan := make(chan string, len(externalTxns))
	var wg sync.WaitGroup

	// Initiate the one-to-many reconciliation process.
	err := s.oneToMany(ctx, externalTxns, matchingRules, isExternalGrouped, &wg, groupCriteria, 100000, matchChan, unmatchedChan)
	if err != nil {
		log.Printf("Error in one-to-many reconciliation: %v", err)
	}

	// Close channels after processing.
	go func() {
		wg.Wait()
		close(matchChan)
		close(unmatchedChan)
	}()

	for match := range matchChan {
		matches = append(matches, match)
	}
	for unmatchedID := range unmatchedChan {
		unmatched = append(unmatched, unmatchedID)
	}

	return matches, unmatched
}

// manyToOneReconciliation performs a many-to-one reconciliation, where multiple internal transactions
// are grouped and matched against a single external transaction.
// Similar parameters as `oneToManyReconciliation`, but operates in reverse (many internal to one external).
// Returns:
// - []model.Match: A list of matched transactions.
// - []string: A list of unmatched transaction IDs.
func (s *Blnk) manyToOneReconciliation(ctx context.Context, internalTxns []*model.Transaction, groupCriteria string, matchingRules []model.MatchingRule, isExternalGrouped bool) ([]model.Match, []string) {
	var matches []model.Match
	var unmatched []string

	matchChan := make(chan model.Match, len(internalTxns))
	unmatchedChan := make(chan string, len(internalTxns))
	var wg sync.WaitGroup

	// Initiate the many-to-one reconciliation process.
	err := s.manyToOne(ctx, internalTxns, matchingRules, isExternalGrouped, &wg, groupCriteria, 100000, matchChan, unmatchedChan)
	if err != nil {
		log.Printf("Error in many-to-one reconciliation: %v", err)
	}

	// Close channels after processing.
	go func() {
		wg.Wait()
		close(matchChan)
		close(unmatchedChan)
	}()

	for match := range matchChan {
		matches = append(matches, match)
	}
	for unmatchedID := range unmatchedChan {
		unmatched = append(unmatched, unmatchedID)
	}

	return matches, unmatched
}

// manyToOne handles the core logic of many-to-one reconciliation.
// It groups external transactions and processes them in batches, comparing them to internal transactions.
// Parameters:
// - ctx: The context controlling the operation.
// - internalTxns: The internal transactions to match against.
// - matchingRules: The rules to apply during reconciliation.
// - isExternalGrouped: Whether the external transactions are grouped or not.
// - wg: The WaitGroup to manage goroutines.
// - groupingCriteria: The criteria used to group transactions.
// - batchSize: The size of each batch of transactions to process.
// - matchChan: Channel to collect matched transactions.
// - unMatchChan: Channel to collect unmatched transactions.
// Returns:
// - error: If any error occurs during processing.
func (s *Blnk) manyToOne(ctx context.Context, internalTxns []*model.Transaction, matchingRules []model.MatchingRule, isExternalGrouped bool, wg *sync.WaitGroup, groupingCriteria string, batchSize int, matchChan chan model.Match, unMatchChan chan string) error {
	offset := int64(0)
	ctx, span := otel.Tracer("blnk.reconciliation").Start(ctx, "ProcessManyToOne")
	defer span.End()

	// Loop to process transactions in batches.
	for {
		groupedExternalTxns, err := s.groupExternalTransactions(ctx, groupingCriteria, batchSize, offset)
		if err != nil {
			log.Printf("Error grouping external transactions: %v", err)
			break
		}
		if len(groupedExternalTxns) == 0 {
			span.AddEvent("No more grouped transactions to process")
			break
		}
		groupMap := s.buildGroupMap(groupedExternalTxns)
		err = s.processGroupedTransactions(internalTxns, groupedExternalTxns, groupMap, matchingRules, isExternalGrouped, wg, matchChan, unMatchChan)
		if err != nil {
			log.Printf("Error processing grouped transactions: %v", err)
		}
		if len(groupMap) == 0 {
			break
		}
		offset += int64(batchSize)
	}
	return nil
}

// groupExternalTransactions retrieves and groups external transactions based on the specified criteria.
// This is used in many-to-one reconciliation.
// Parameters:
// - ctx: The context controlling the operation.
// - groupingCriteria: The criteria to group transactions.
// - batchSize: The number of transactions to process in each batch.
// - offset: The offset for pagination.
// Returns:
// - map[string][]*model.Transaction: A map of grouped transactions.
// - error: If there is an error retrieving the transactions.
func (s *Blnk) groupExternalTransactions(ctx context.Context, groupingCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	return s.datasource.FetchAndGroupExternalTransactions(ctx, "", groupingCriteria, batchSize, offset)
}

// processGroupedTransactions handles the matching of transactions for both one-to-many and many-to-one reconciliations.
// It processes each transaction group in parallel, comparing them to the target transactions.
// Parameters:
// - singleTxns: The single transactions (either external or internal).
// - groupedTxns: The grouped transactions (either internal or external).
// - groupMap: A map indicating the groups that have not yet been matched.
// - matchingRules: The rules to apply for matching transactions.
// - isExternalGrouped: Whether the external transactions are grouped.
// - wg: The WaitGroup managing goroutines.
// - matchChan: Channel for collecting matches.
// - unMatchChan: Channel for collecting unmatched transaction IDs.
// Returns:
// - error: If any error occurs during processing.
func (s *Blnk) processGroupedTransactions(singleTxns []*model.Transaction, groupedTxns map[string][]*model.Transaction, groupMap map[string]bool, matchingRules []model.MatchingRule, isExternalGrouped bool, wg *sync.WaitGroup, matchChan chan model.Match, unMatchChan chan string) error {
	for _, singleTxn := range singleTxns {
		wg.Add(1)
		go func(txn *model.Transaction) {
			defer wg.Done()
			matched := s.matchSingleTransaction(txn, groupedTxns, groupMap, matchingRules, isExternalGrouped, matchChan)
			if !matched {
				unMatchChan <- txn.TransactionID
			}
		}(singleTxn)
	}

	return nil
}

// matchSingleTransaction attempts to match a single transaction with a group of transactions based on matching rules.
// If a match is found, it sends the match to the match channel and marks the group as processed.
// Parameters:
// - singleTxn: The single transaction to match.
// - groupedTxns: The grouped transactions to compare against.
// - groupMap: A map tracking unprocessed groups.
// - matchingRules: The rules for matching transactions.
// - isExternalGrouped: Whether the external transactions are grouped.
// - matchChan: Channel for sending matches.
// Returns:
// - bool: True if a match is found, false otherwise.
func (s *Blnk) matchSingleTransaction(singleTxn *model.Transaction, groupedTxns map[string][]*model.Transaction, groupMap map[string]bool, matchingRules []model.MatchingRule, isExternalGrouped bool, matchChan chan model.Match) bool {
	for groupKey := range groupMap {
		if s.matchesGroup(singleTxn, groupedTxns[groupKey], matchingRules) {
			for _, groupedTxn := range groupedTxns[groupKey] {
				var externalID, internalID string
				if isExternalGrouped {
					externalID = groupedTxn.TransactionID
					internalID = singleTxn.TransactionID
				} else {
					externalID = singleTxn.TransactionID
					internalID = groupedTxn.TransactionID
				}
				// Send the match result to the channel.
				matchChan <- model.Match{
					ExternalTransactionID: externalID,
					InternalTransactionID: internalID,
					Amount:                groupedTxn.Amount,
					Date:                  groupedTxn.CreatedAt,
				}
			}
			delete(groupMap, groupKey) // Mark the group as processed.
			return true
		}
	}
	return false
}

// oneToMany performs the core logic for one-to-many reconciliation.
// Parameters are similar to `manyToOne`, but it processes transactions in reverse (one external to many internal).
// Returns:
// - error: If any error occurs during processing.
func (s *Blnk) oneToMany(ctx context.Context, singleTxn []*model.Transaction, matchingRules []model.MatchingRule, isExternalGrouped bool, wg *sync.WaitGroup, groupingCriteria string, batchSize int, matchChan chan model.Match, unMatchChan chan string) error {
	offset := int64(0)
	ctx, span := otel.Tracer("blnk.reconciliation").Start(ctx, "ProcessOneToMany")
	defer span.End()

	for {
		txns, err := s.groupInternalTransactions(ctx, groupingCriteria, batchSize, offset)
		if err != nil {
			log.Printf("Error grouping internal transactions: %v", err)
			break
		}
		if len(txns) == 0 {
			span.AddEvent("No more grouped transactions to process")
			break
		}
		groupMap := s.buildGroupMap(txns)
		err = s.processGroupedTransactions(singleTxn, txns, groupMap, matchingRules, isExternalGrouped, wg, matchChan, unMatchChan)
		if err != nil {
			log.Printf("Error in one-to-many reconciliation: %v", err)
		}
		if len(groupMap) == 0 {
			break
		}
		offset += int64(batchSize)
	}
	return nil
}

// buildGroupMap creates a map for tracking unprocessed transaction groups.
// Parameters:
// - groupedTxns: The grouped transactions.
// Returns:
// - map[string]bool: A map with group keys as true, indicating that they have not yet been processed.
func (s *Blnk) buildGroupMap(groupedTxns map[string][]*model.Transaction) map[string]bool {
	groupMap := make(map[string]bool)
	for key := range groupedTxns {
		groupMap[key] = true
	}
	return groupMap
}

// groupInternalTransactions groups internal transactions based on the specified criteria for reconciliation.
// Parameters are the same as `groupExternalTransactions`.
// Returns:
// - map[string][]*model.Transaction: A map of grouped internal transactions.
// - error: If any error occurs during grouping.
func (s *Blnk) groupInternalTransactions(ctx context.Context, groupingCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	return s.datasource.GroupTransactions(ctx, groupingCriteria, batchSize, offset)
}

// findMatchingInternalTransaction attempts to find an internal transaction that matches the given external transaction.
// It processes the transactions in batches and applies the matching rules.
// Parameters:
// - ctx: The context controlling the operation.
// - externalTxn: The external transaction to match.
// - matchingRules: The rules for matching transactions.
// - matchChan: Channel to send matched transactions.
// - unMatchChan: Channel to send unmatched transaction IDs.
// Returns:
// - error: If any error occurs during processing.
func (s *Blnk) findMatchingInternalTransaction(ctx context.Context, externalTxn *model.Transaction, matchingRules []model.MatchingRule, matchChan chan model.Match, unMatchChan chan string) error {
	matchFound := false

	// Process transactions in batches, applying the matching rules.
	_, err := s.ProcessTransactionInBatches(
		ctx,
		externalTxn.TransactionID,
		externalTxn.Amount,
		10,    // Batch size
		false, // Stream mode
		s.getInternalTransactionsPaginated,
		func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
			defer wg.Done()
			for internalTxn := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
					if s.matchesRules(externalTxn, *internalTxn, matchingRules) {
						matchChan <- model.Match{
							ExternalTransactionID: externalTxn.TransactionID,
							InternalTransactionID: internalTxn.TransactionID,
							Amount:                externalTxn.Amount,
							Date:                  externalTxn.CreatedAt,
						}
						matchFound = true
						return
					}
				}
			}
		},
	)
	if err != nil && err != context.Canceled {
		return err
	}

	if !matchFound {
		select {
		case unMatchChan <- externalTxn.TransactionID:
		default:
			return fmt.Errorf("failed to send unmatched transaction ID to channel")
		}
	}

	return nil
}

// getExternalTransactionsPaginated retrieves paginated external transactions from the data source.
// Parameters:
// - ctx: The context controlling the request.
// - uploadID: The ID of the upload to retrieve transactions for.
// - limit: The maximum number of transactions to retrieve.
// - offset: The offset for pagination.
// Returns:
// - []*model.Transaction: A list of external transactions converted to internal transactions.
// - error: If any error occurs during retrieval.
func (s *Blnk) getExternalTransactionsPaginated(ctx context.Context, uploadID string, limit int, offset int64) ([]*model.Transaction, error) {
	log.Printf("Fetching external transactions: uploadID=%s, limit=%d, offset=%d", uploadID, limit, offset)
	externalTransaction, err := s.datasource.GetExternalTransactionsPaginated(ctx, uploadID, limit, int64(offset))
	if err != nil {
		log.Printf("Error fetching external transactions: %v", err)
		return nil, err
	}
	log.Printf("Fetched %d external transactions", len(externalTransaction))
	transactions := make([]*model.Transaction, len(externalTransaction))

	for i, txn := range externalTransaction {
		transactions[i] = txn.ToInternalTransaction()
	}
	return transactions, nil
}

// getInternalTransactionsPaginated retrieves paginated internal transactions from the data source.
// Parameters:
// - ctx: The context controlling the request.
// - id: The ID of the internal transactions to retrieve.
// - limit: The maximum number of transactions to retrieve.
// - offset: The offset for pagination.
// Returns:
// - []*model.Transaction: A list of internal transactions.
// - error: If any error occurs during retrieval.
func (s *Blnk) getInternalTransactionsPaginated(ctx context.Context, id string, limit int, offset int64) ([]*model.Transaction, error) {
	return s.datasource.GetTransactionsPaginated(ctx, "", limit, offset)
}

// matchesGroup compares a group of internal transactions with a single external transaction using matching rules.
// If a match is found, it returns true; otherwise, it returns false.
// Parameters:
// - externalTxn: The external transaction to compare.
// - group: The group of internal transactions to compare against.
// - matchingRules: The rules for matching transactions.
// Returns:
// - bool: True if the group matches the external transaction, false otherwise.
func (s *Blnk) matchesGroup(externalTxn *model.Transaction, group []*model.Transaction, matchingRules []model.MatchingRule) bool {
	var totalAmount float64
	var minDate, maxDate time.Time
	descriptions := make([]string, 0, len(group))
	references := make([]string, 0, len(group))
	currencies := make(map[string]bool)

	// Iterate over the group of internal transactions and accumulate information.
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

	// Create a virtual transaction representing the group for comparison.
	groupTxn := model.Transaction{
		Amount:      totalAmount,
		CreatedAt:   minDate, // Use the earliest date in the group.
		Description: strings.Join(descriptions, " | "),
		Reference:   strings.Join(references, " | "),
		Currency:    s.dominantCurrency(currencies), // Determine the dominant currency in the group.
	}

	// Use the matching rules to compare the group with the external transaction.
	return s.matchesRules(externalTxn, groupTxn, matchingRules)
}

// dominantCurrency returns the dominant currency in a group of transactions.
// If there is only one currency, it returns that currency, otherwise, it returns "MIXED".
func (s *Blnk) dominantCurrency(currencies map[string]bool) string {
	if len(currencies) == 1 {
		for currency := range currencies {
			return currency // Return the only currency if there's exactly one.
		}
	}
	return "MIXED" // Return "MIXED" if there are multiple currencies.
}

// CreateMatchingRule creates a new matching rule after validating it.
// Parameters:
// - ctx: The context for managing the request.
// - rule: The matching rule to be created.
// Returns the created rule, or an error if validation or storage fails.
func (s *Blnk) CreateMatchingRule(ctx context.Context, rule model.MatchingRule) (*model.MatchingRule, error) {
	rule.RuleID = model.GenerateUUIDWithSuffix("rule") // Generate a unique rule ID.
	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()

	// Validate the rule before storing it.
	err := s.validateRule(&rule)
	if err != nil {
		return nil, err
	}

	// Store the rule in the datasource.
	err = s.datasource.RecordMatchingRule(ctx, &rule)
	if err != nil {
		return nil, err
	}

	return &rule, nil
}

// GetMatchingRule retrieves a matching rule by its ID.
// Parameters:
// - ctx: The context for managing the request.
// - id: The ID of the matching rule to retrieve.
// Returns the matching rule, or an error if retrieval fails.
func (s *Blnk) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	rule, err := s.datasource.GetMatchingRule(ctx, id)
	if err != nil {
		return nil, err
	}
	return rule, nil
}

// UpdateMatchingRule updates an existing matching rule.
// It retrieves the existing rule, validates the new data, and then updates the rule.
// Parameters:
// - ctx: The context for managing the request.
// - rule: The updated rule data.
// Returns the updated rule, or an error if validation or update fails.
func (s *Blnk) UpdateMatchingRule(ctx context.Context, rule model.MatchingRule) (*model.MatchingRule, error) {
	// Retrieve the existing rule by its ID.
	existingRule, err := s.GetMatchingRule(ctx, rule.RuleID)
	if err != nil {
		return nil, err
	}

	// Preserve the original creation time and update the modified time.
	rule.CreatedAt = existingRule.CreatedAt
	rule.UpdatedAt = time.Now()

	// Validate the updated rule.
	err = s.validateRule(&rule)
	if err != nil {
		return nil, err
	}

	// Update the rule in the datasource.
	err = s.datasource.UpdateMatchingRule(ctx, &rule)
	if err != nil {
		return nil, err
	}

	return &rule, nil
}

// DeleteMatchingRule deletes a matching rule by its ID.
// Parameters:
// - ctx: The context for managing the request.
// - id: The ID of the rule to delete.
// Returns an error if deletion fails.
func (s *Blnk) DeleteMatchingRule(ctx context.Context, id string) error {
	return s.datasource.DeleteMatchingRule(ctx, id)
}

// ListMatchingRules retrieves all matching rules.
// Parameters:
// - ctx: The context for managing the request.
// Returns a list of matching rules, or an error if retrieval fails.
func (s *Blnk) ListMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	return s.datasource.GetMatchingRules(ctx)
}

// validateRule validates a matching rule, including checking its basic structure and criteria.
// Parameters:
// - rule: The rule to validate.
// Returns an error if the rule or any of its criteria are invalid.
func (s *Blnk) validateRule(rule *model.MatchingRule) error {
	// Validate basic structure of the rule.
	if err := s.validateRuleBasics(rule); err != nil {
		return err
	}

	// Validate each individual criterion.
	for _, criteria := range rule.Criteria {
		if err := s.validateCriteria(criteria); err != nil {
			return err
		}
	}

	return nil
}

// validateRuleBasics checks that the rule has a valid name and at least one criterion.
// Parameters:
// - rule: The rule to validate.
// Returns an error if the rule is missing required fields.
func (s *Blnk) validateRuleBasics(rule *model.MatchingRule) error {
	if rule.Name == "" {
		return errors.New("rule name is required")
	}

	if len(rule.Criteria) == 0 {
		return errors.New("at least one matching criteria is required")
	}

	return nil
}

// validateCriteria checks the validity of a criterion, including its field, operator, and drift values.
// Parameters:
// - criteria: The criteria to validate.
// Returns an error if the criteria are invalid.
func (s *Blnk) validateCriteria(criteria model.MatchingCriteria) error {
	if criteria.Field == "" || criteria.Operator == "" {
		return errors.New("field and operator are required for each criteria")
	}

	if err := s.validateOperator(criteria.Operator); err != nil {
		return err
	}

	if err := s.validateField(criteria.Field); err != nil {
		return err
	}

	return s.validateDrift(criteria)
}

// validateOperator checks if the provided operator is valid.
// Parameters:
// - operator: The operator to validate.
// Returns an error if the operator is invalid.
func (s *Blnk) validateOperator(operator string) error {
	validOperators := []string{"equals", "greater_than", "less_than", "contains"}
	if !contains(validOperators, operator) {
		return errors.New("invalid operator")
	}
	return nil
}

// validateField checks if the provided field is valid for a matching rule.
// Parameters:
// - field: The field to validate.
// Returns an error if the field is invalid.
func (s *Blnk) validateField(field string) error {
	validFields := []string{"amount", "date", "description", "reference", "currency"}
	if !contains(validFields, field) {
		return errors.New("invalid field")
	}
	return nil
}

// validateDrift checks the allowable drift for the given field and operator.
// Drift refers to acceptable deviations (e.g., percentage for amounts or seconds for dates).
// Parameters:
// - criteria: The criteria to validate.
// Returns an error if the drift is invalid.
func (s *Blnk) validateDrift(criteria model.MatchingCriteria) error {
	if criteria.Operator == "equals" {
		switch criteria.Field {
		case "amount":
			if criteria.AllowableDrift < 0 || criteria.AllowableDrift > 100 {
				return errors.New("drift for amount must be between 0 and 100 (percentage)")
			}
		case "date":
			if criteria.AllowableDrift < 0 {
				return errors.New("drift for date must be non-negative (seconds)")
			}
		}
	}
	return nil
}

// getMatchingRules retrieves a list of matching rules by their IDs.
// Parameters:
// - ctx: The context for managing the request.
// - matchingRuleIDs: The list of matching rule IDs to retrieve.
// Returns a list of matching rules, or an error if retrieval fails.
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

// matchesString compares the external and internal string values based on the matching criteria.
// Parameters:
// - externalValue: The value from the external transaction.
// - internalValue: The value from the internal transaction.
// - criteria: The matching criteria, including operator and allowable drift.
// Returns true if the values match according to the criteria, otherwise false.
func (s *Blnk) matchesString(externalValue, internalValue string, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		// Check if any part of the internal value matches the external value exactly.
		for _, part := range strings.Split(internalValue, " | ") {
			if strings.EqualFold(externalValue, part) {
				return true
			}
		}
		return false
	case "contains":
		// Check if the external value is contained in any part of the internal value.
		for _, part := range strings.Split(internalValue, " | ") {
			if s.partialMatch(externalValue, part, criteria.AllowableDrift) {
				return true
			}
		}
		return false
	}
	return false
}

// partialMatch compares two strings and checks if they match within a certain allowable drift, using Levenshtein distance.
// Parameters:
// - str1, str2: The strings to compare.
// - allowableDrift: The allowable difference between the two strings (as a percentage).
// Returns true if the strings match within the allowable drift, otherwise false.
func (s *Blnk) partialMatch(str1, str2 string, allowableDrift float64) bool {
	str1 = strings.ToLower(str1) // Convert to lowercase for case-insensitive comparison.
	str2 = strings.ToLower(str2)

	// Check if either string contains the other.
	if strings.Contains(str1, str2) || strings.Contains(str2, str1) {
		return true
	}

	// Calculate the Levenshtein distance between the strings.
	distance := levenshtein.DistanceForStrings([]rune(str1), []rune(str2), levenshtein.DefaultOptions)

	// Calculate the maximum allowable distance based on the length of the longer string.
	maxLength := float64(max(len(str1), len(str2)))
	maxAllowedDistance := int(maxLength * (allowableDrift / 100))

	// Return true if the distance is within the allowable drift.
	return distance <= maxAllowedDistance
}

// matchesCurrency compares currency values, handling special cases like "MIXED" for group transactions.
// Parameters:
// - externalValue: The currency value from the external transaction.
// - internalValue: The currency value from the internal transaction.
// - criteria: The matching criteria.
// Returns true if the currencies match, otherwise false.
func (s *Blnk) matchesCurrency(externalValue, internalValue string, criteria model.MatchingCriteria) bool {
	if internalValue == "MIXED" {
		// Handle the special case where the internal value is "MIXED" (multiple currencies in a group).
		return true
	}
	return s.matchesString(externalValue, internalValue, criteria)
}

// matchesGroupAmount compares the amount of a single external transaction with the total amount of a grouped set of internal transactions.
// Parameters:
// - externalAmount: The amount from the external transaction.
// - groupAmount: The total amount from the group of internal transactions.
// - criteria: The matching criteria, including operator and allowable drift.
// Returns true if the amounts match according to the criteria, otherwise false.
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

// matchesGroupDate compares the date of a single external transaction with the earliest date in a group of internal transactions.
// Parameters:
// - externalDate: The date from the external transaction.
// - groupEarliestDate: The earliest date from the group of internal transactions.
// - criteria: The matching criteria, including operator and allowable drift.
// Returns true if the dates match according to the criteria, otherwise false.
func (s *Blnk) matchesGroupDate(externalDate, groupEarliestDate time.Time, criteria model.MatchingCriteria) bool {
	switch criteria.Operator {
	case "equals":
		difference := externalDate.Sub(groupEarliestDate)
		return math.Abs(float64(difference/time.Second)) <= criteria.AllowableDrift
	case "after":
		return externalDate.After(groupEarliestDate)
	case "before":
		return externalDate.Before(groupEarliestDate)
	}
	return false
}
