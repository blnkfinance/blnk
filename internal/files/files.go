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

package files

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// StoreFunc defines the function signature for storing transactions.
type StoreFunc func(ctx context.Context, uploadID string, txn model.ExternalTransaction) error

// UploadExternalData handles the process of uploading external data by detecting file type, parsing, and storing it.
// Parameters:
// - ctx: The context for controlling execution.
// - source: The source of the external data.
// - reader: An io.Reader for reading the data.
// - filename: The name of the file being uploaded.
// - store: The callback function to store processed transactions.
// Returns:
// - string: The ID of the upload.
// - int: The total number of records processed.
// - error: If any step of the process fails.
func UploadExternalData(ctx context.Context, source string, reader io.Reader, filename string, store StoreFunc) (string, int, error) {
	uploadID := model.GenerateUUIDWithSuffix("upload") // Generate a unique ID for the upload.

	// Create a temporary file and populate it with the uploaded data.
	tempFile, err := createAndPopulateTempFile(filename, reader)
	if err != nil {
		return "", 0, err
	}
	defer cleanupTempFile(tempFile) // Ensure the temp file is cleaned up after processing.

	// Detect the file type (CSV or JSON) based on the content.
	fileType, err := detectFileTypeFromTempFile(tempFile, filename)
	if err != nil {
		return "", 0, err
	}

	// Parse and store the data based on its file type.
	total, err := parseAndStoreData(ctx, uploadID, source, tempFile, fileType, store)
	if err != nil {
		return "", 0, err
	}

	return uploadID, total, nil
}

// createAndPopulateTempFile creates a temporary file and writes the uploaded data to it.
func createAndPopulateTempFile(filename string, reader io.Reader) (*os.File, error) {
	// Create a new temporary file with a name based on the original filename.
	tempFile, err := createTempFile(filename)
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
func detectFileTypeFromTempFile(tempFile *os.File, filename string) (string, error) {
	header := make([]byte, 512)
	// Read the first 512 bytes of the file (enough for MIME type detection).
	if _, err := tempFile.Read(header); err != nil && err != io.EOF {
		return "", fmt.Errorf("error reading file header: %w", err)
	}

	// Detect the file type based on the content.
	fileType, err := DetectFileType(header, filename)
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
func parseAndStoreData(ctx context.Context, uploadID, source string, reader io.Reader, fileType string, store StoreFunc) (int, error) {
	switch fileType {
	case "text/csv", "text/csv; charset=utf-8":
		// Handle CSV files.
		err := ProcessCSV(ctx, uploadID, source, reader, store)
		return 0, err // CSV parsing doesn't return a count in the original implementation logic for count in ProcessCSVRows return
	case "application/json":
		// Handle JSON files.
		return ProcessJSON(ctx, uploadID, source, reader, store)
	default:
		// Return an error if the file type is unsupported.
		return 0, fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// createTempFile creates a new temporary file for storing the uploaded data.
func createTempFile(originalFilename string) (*os.File, error) {
	// Create the directory for temporary files if it doesn't exist.
	tempDir := filepath.Join(os.TempDir(), "blnk_uploads")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
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
func cleanupTempFile(file *os.File) {
	if file != nil {
		filename := file.Name() // Get the file name.
		file.Close()            // Close the file before removing it.
		if err := os.Remove(filename); err != nil {
			// Log any errors encountered during file removal.
			log.Printf("Error removing temporary file %s: %v", filename, err)
		}
	}
}

// DetectFileType attempts to detect the file type based on its extension or content.
// If the file extension can identify the type, it returns that, otherwise, it inspects the content of the file.
func DetectFileType(data []byte, filename string) (string, error) {
	// Attempt to detect file type by its extension first.
	if mimeType := DetectByExtension(filename); mimeType != "" {
		return mimeType, nil
	}
	// If detection by extension fails, analyze the content.
	return DetectByContent(data)
}

// DetectByExtension detects the MIME type by the file extension.
func DetectByExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename)) // Extract and lower the file extension.
	return mime.TypeByExtension(ext)               // Use the standard library to get MIME type.
}

// DetectByContent detects the MIME type based on the content of the file.
func DetectByContent(data []byte) (string, error) {
	mimeType := http.DetectContentType(data) // Detect content type by analyzing the first 512 bytes.

	switch mimeType {
	case "application/octet-stream", "text/plain":
		// If detected as binary or plain text, analyze the content further.
		return AnalyzeTextContent(data)
	case "text/csv; charset=utf-8":
		// Directly return if CSV is detected.
		return "text/csv", nil
	default:
		return mimeType, nil // Return detected MIME type.
	}
}

// AnalyzeTextContent further inspects text-based content to differentiate between CSV, JSON, or plain text.
func AnalyzeTextContent(data []byte) (string, error) {
	if LooksLikeCSV(data) {
		return "text/csv", nil
	}
	if json.Valid(data) {
		return "application/json", nil
	}
	return "text/plain", nil // Default to plain text if no other format matches.
}

// LooksLikeCSV checks whether the provided data looks like a CSV file.
func LooksLikeCSV(data []byte) bool {
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

// ProcessCSV reads and processes a CSV file from an io.Reader, parsing each row and storing the corresponding transactions.
func ProcessCSV(ctx context.Context, uploadID, source string, reader io.Reader, store StoreFunc) error {
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
	return processCSVRows(ctx, uploadID, source, csvReader, columnMap, store)
}

// processCSVRows reads and processes each row in the CSV file, parsing the fields and storing the transactions.
func processCSVRows(ctx context.Context, uploadID, source string, csvReader *csv.Reader, columnMap map[string]int, store StoreFunc) error {
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
		if err := store(ctx, uploadID, externalTxn); err != nil {
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

// ProcessJSON parses and stores transactions from a JSON file.
func ProcessJSON(ctx context.Context, uploadID, source string, reader io.Reader, store StoreFunc) (int, error) {
	decoder := json.NewDecoder(reader)
	var transactions []model.ExternalTransaction
	// Decode the JSON data into a slice of ExternalTransaction objects.
	if err := decoder.Decode(&transactions); err != nil {
		return 0, err
	}

	// Store each parsed transaction.
	for _, txn := range transactions {
		txn.Source = source
		if err := store(ctx, uploadID, txn); err != nil {
			return 0, err
		}
	}

	return len(transactions), nil
}

// parseFloat parses a string into a float64 value.
func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0 // Return 0 if parsing fails.
	}
	return f
}

// parseTime parses a string into a time.Time object in RFC3339 format.
func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{} // Return zero time if parsing fails.
	}
	return t
}

// contains checks whether a slice contains a specific string.
// Not used locally but useful helper; keeping it if needed or can be removed if not used.
func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}
