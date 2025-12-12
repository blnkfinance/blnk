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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// StoreFunc defines the function signature for storing transactions.
type StoreFunc func(ctx context.Context, uploadID string, txn model.ExternalTransaction) error

// Configuration constants for performance tuning
const (
	DefaultBatchSize     = 500
	DefaultWorkerCount   = 4
	DefaultBufferSize    = 64 * 1024 // 64KB buffer
	DefaultChannelSize   = 1000
	ContextCheckInterval = 100
)

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
	uploadID := model.GenerateUUIDWithSuffix("upload")

	// Use buffered reader for better I/O performance
	bufferedReader := bufio.NewReaderSize(reader, DefaultBufferSize)

	tempFile, err := createAndPopulateTempFile(filename, bufferedReader)
	if err != nil {
		return "", 0, err
	}
	defer cleanupTempFile(tempFile)

	fileType, err := detectFileTypeFromTempFile(tempFile, filename)
	if err != nil {
		return "", 0, err
	}

	total, err := parseAndStoreData(ctx, uploadID, source, tempFile, fileType, store)
	if err != nil {
		return "", 0, err
	}

	return uploadID, total, nil
}

func createAndPopulateTempFile(filename string, reader io.Reader) (*os.File, error) {
	tempFile, err := createTempFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error creating temporary file: %w", err)
	}

	// Use buffered writer for faster writes
	writer := bufio.NewWriterSize(tempFile, DefaultBufferSize)
	if _, err := io.Copy(writer, reader); err != nil {
		return nil, fmt.Errorf("error copying upload data: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("error flushing temporary file: %w", err)
	}

	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error seeking temporary file: %w", err)
	}

	return tempFile, nil
}

func detectFileTypeFromTempFile(tempFile *os.File, filename string) (string, error) {
	header := make([]byte, 512)
	if _, err := tempFile.Read(header); err != nil && err != io.EOF {
		return "", fmt.Errorf("error reading file header: %w", err)
	}

	fileType, err := DetectFileType(header, filename)
	if err != nil {
		return "", fmt.Errorf("error detecting file type: %w", err)
	}

	if _, err := tempFile.Seek(0, 0); err != nil {
		return "", fmt.Errorf("error seeking temporary file: %w", err)
	}

	return fileType, nil
}

func parseAndStoreData(ctx context.Context, uploadID, source string, reader io.Reader, fileType string, store StoreFunc) (int, error) {
	switch fileType {
	case "text/csv", "text/csv; charset=utf-8":
		return ProcessCSV(ctx, uploadID, source, reader, store)
	case "application/json":
		return ProcessJSON(ctx, uploadID, source, reader, store)
	default:
		return 0, fmt.Errorf("unsupported file type: %s", fileType)
	}
}

func createTempFile(originalFilename string) (*os.File, error) {
	tempDir := filepath.Join(os.TempDir(), "blnk_uploads")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, fmt.Errorf("error creating temporary directory: %w", err)
	}

	prefix := fmt.Sprintf("%s_", filepath.Base(originalFilename))
	tempFile, err := os.CreateTemp(tempDir, prefix)
	if err != nil {
		return nil, fmt.Errorf("error creating temporary file: %w", err)
	}

	return tempFile, nil
}

func cleanupTempFile(file *os.File) {
	if file != nil {
		filename := file.Name()
		if err := file.Close(); err != nil {
			log.Printf("Error closing temporary file %s: %v", filename, err)
		}
		if err := os.Remove(filename); err != nil {
			log.Printf("Error removing temporary file %s: %v", filename, err)
		}
	}
}

func DetectFileType(data []byte, filename string) (string, error) {
	if mimeType := DetectByExtension(filename); mimeType != "" {
		return mimeType, nil
	}
	return DetectByContent(data)
}

func DetectByExtension(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	return mime.TypeByExtension(ext)
}

func DetectByContent(data []byte) (string, error) {
	mimeType := http.DetectContentType(data)

	switch mimeType {
	case "application/octet-stream", "text/plain":
		return AnalyzeTextContent(data)
	case "text/csv; charset=utf-8":
		return "text/csv", nil
	default:
		return mimeType, nil
	}
}

func AnalyzeTextContent(data []byte) (string, error) {
	if LooksLikeCSV(data) {
		return "text/csv", nil
	}
	if json.Valid(data) {
		return "application/json", nil
	}
	return "text/plain", nil
}

func LooksLikeCSV(data []byte) bool {
	lines := bytes.Split(data, []byte("\n"))
	if len(lines) < 2 {
		return false
	}

	fields := bytes.Count(lines[0], []byte(",")) + 1
	// Only check first few lines for performance
	maxLinesToCheck := 5
	if len(lines) < maxLinesToCheck {
		maxLinesToCheck = len(lines)
	}

	for _, line := range lines[1:maxLinesToCheck] {
		if len(line) == 0 {
			continue
		}
		if bytes.Count(line, []byte(","))+1 != fields {
			return false
		}
	}

	return fields > 1
}

// ProcessCSV now uses parallel processing with workers
func ProcessCSV(ctx context.Context, uploadID, source string, reader io.Reader, store StoreFunc) (int, error) {
	bufferedReader := bufio.NewReaderSize(reader, DefaultBufferSize)
	csvReader := csv.NewReader(bufferedReader)
	csvReader.ReuseRecord = true // Reuse slice to reduce allocations

	headers, err := csvReader.Read()
	if err != nil {
		return 0, fmt.Errorf("error reading CSV headers: %w", err)
	}

	// Make a copy of headers since ReuseRecord is enabled
	headersCopy := make([]string, len(headers))
	copy(headersCopy, headers)

	columnMap, err := createColumnMap(headersCopy)
	if err != nil {
		return 0, err
	}

	return processCSVRowsParallel(ctx, uploadID, source, csvReader, columnMap, store)
}

// Parallel CSV processing with worker pool
func processCSVRowsParallel(ctx context.Context, uploadID, source string, csvReader *csv.Reader, columnMap map[string]int, store StoreFunc) (int, error) {
	workerCount := runtime.NumCPU()
	if workerCount > DefaultWorkerCount {
		workerCount = DefaultWorkerCount
	}

	rowChan := make(chan []string, DefaultChannelSize)
	errChan := make(chan error, workerCount)
	var wg sync.WaitGroup
	totalCount := int64(0)
	var countMu sync.Mutex

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processWorker(ctx, uploadID, source, rowChan, columnMap, store, &totalCount, &countMu, errChan)
		}()
	}

	// Read CSV rows and send to workers
	go func() {
		rowNum := 1
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errChan <- fmt.Errorf("error reading row %d: %w", rowNum, err)
				continue
			}

			rowNum++

			// Make a copy since csvReader.ReuseRecord is true
			recordCopy := make([]string, len(record))
			copy(recordCopy, record)

			select {
			case <-ctx.Done():
				close(rowChan)
				return
			case rowChan <- recordCopy:
			}
		}
		close(rowChan)
	}()

	// Wait for workers to finish
	wg.Wait()
	close(errChan)

	// Collect errors
	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return int(totalCount), fmt.Errorf("encountered %d errors while processing CSV: %v", len(errs), errs)
	}

	return int(totalCount), nil
}

func processWorker(ctx context.Context, uploadID, source string, rowChan <-chan []string, columnMap map[string]int, store StoreFunc, totalCount *int64, countMu *sync.Mutex, errChan chan<- error) {
	batch := make([]model.ExternalTransaction, 0, DefaultBatchSize)
	processCount := 0

	for record := range rowChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		externalTxn, err := parseExternalTransaction(record, columnMap, source)
		if err != nil {
			errChan <- err
			continue
		}

		batch = append(batch, externalTxn)

		// Process batch when full
		if len(batch) >= DefaultBatchSize {
			if err := storeBatch(ctx, uploadID, batch, store); err != nil {
				errChan <- err
			} else {
				countMu.Lock()
				*totalCount += int64(len(batch))
				countMu.Unlock()
			}
			batch = batch[:0] // Reset batch
		}

		processCount++
		if processCount%ContextCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	// Process remaining items in batch
	if len(batch) > 0 {
		if err := storeBatch(ctx, uploadID, batch, store); err != nil {
			errChan <- err
		} else {
			countMu.Lock()
			*totalCount += int64(len(batch))
			countMu.Unlock()
		}
	}
}

func storeBatch(ctx context.Context, uploadID string, batch []model.ExternalTransaction, store StoreFunc) error {
	for _, txn := range batch {
		if err := store(ctx, uploadID, txn); err != nil {
			return err
		}
	}
	return nil
}

func createColumnMap(headers []string) (map[string]int, error) {
	requiredColumns := []string{"ID", "Amount", "Date"}
	columnMap := make(map[string]int, len(headers))

	for i, header := range headers {
		normalized := strings.ToLower(strings.TrimSpace(header))
		columnMap[normalized] = i
	}

	for _, col := range requiredColumns {
		if _, exists := columnMap[strings.ToLower(col)]; !exists {
			return nil, fmt.Errorf("required column '%s' not found in CSV", col)
		}
	}

	return columnMap, nil
}

func parseExternalTransaction(record []string, columnMap map[string]int, source string) (model.ExternalTransaction, error) {
	if len(record) != len(columnMap) {
		return model.ExternalTransaction{}, fmt.Errorf("incorrect number of fields in record")
	}

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

// ProcessJSON now uses batch processing
func ProcessJSON(ctx context.Context, uploadID, source string, reader io.Reader, store StoreFunc) (int, error) {
	bufferedReader := bufio.NewReaderSize(reader, DefaultBufferSize)
	decoder := json.NewDecoder(bufferedReader)

	var transactions []model.ExternalTransaction
	if err := decoder.Decode(&transactions); err != nil {
		return 0, err
	}

	// Process in batches for better performance
	batchSize := DefaultBatchSize
	totalProcessed := 0

	for i := 0; i < len(transactions); i += batchSize {
		end := i + batchSize
		if end > len(transactions) {
			end = len(transactions)
		}

		batch := transactions[i:end]
		for j := range batch {
			batch[j].Source = source
			if err := store(ctx, uploadID, batch[j]); err != nil {
				return totalProcessed, err
			}
		}

		totalProcessed += len(batch)

		// Check context periodically
		if i%ContextCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return totalProcessed, ctx.Err()
			default:
			}
		}
	}

	return len(transactions), nil
}

func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
