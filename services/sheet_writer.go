package services

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type GoogleSheetsWriter struct {
	sheetsService    *sheets.Service
	spreadsheetID    string
	retryMaxAttempts int
	retryDelay       time.Duration
}

func NewGoogleSheetsWriter(spreadsheetID string, credentialsFilePath string, retryMaxAttempts int, retryDelay time.Duration) (*GoogleSheetsWriter, error) {
	ctx := context.Background()
	credentialsJSON, err := os.ReadFile(credentialsFilePath)

	if err != nil {
		return nil, fmt.Errorf("failed to read credentials file: %w", err)
	}

	config, err := google.JWTConfigFromJSON(credentialsJSON, sheets.SpreadsheetsScope)
	if err != nil {
		return nil, fmt.Errorf("failed to configure JWT from credentials: %w", err)
	}

	client := config.Client(ctx)
	sheetsService, err := sheets.NewService(ctx, option.WithHTTPClient(client))

	if err != nil {
		return nil, fmt.Errorf("failed to create Google Sheets API client: %w", err)
	}

	return &GoogleSheetsWriter{
		sheetsService:    sheetsService,
		spreadsheetID:    spreadsheetID,
		retryMaxAttempts: retryMaxAttempts,
		retryDelay:       retryDelay,
	}, nil
}

func (w *GoogleSheetsWriter) Clear(ctx context.Context, sheetName string) error {
	clearRange := fmt.Sprintf("'%s'!A1:ZZ", sheetName)
	req := sheets.ClearValuesRequest{}
	log.Printf("API Sheets: Clearing range '%s' in spreadsheet '%s'...", clearRange, w.spreadsheetID)
	_, err := w.sheetsService.Spreadsheets.Values.Clear(w.spreadsheetID, clearRange, &req).Context(context.Background()).Do()

	if err != nil {
		return fmt.Errorf("failed to clear range '%s' in spreadsheet '%s': %w", clearRange, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: Range '%s' cleared successfully.", clearRange)

	return nil
}

func (w *GoogleSheetsWriter) SetHeaders(ctx context.Context, sheetName string, headers []string) error {
	writeRange := fmt.Sprintf("'%s'!A1", sheetName)
	values := [][]interface{}{{}}

	for _, h := range headers {
		values[0] = append(values[0], h)
	}

	valueInputOption := "USER_ENTERED"
	log.Printf("API Sheets: Setting headers at '%s'!A1 in spreadsheet '%s' (with context)...", sheetName, w.spreadsheetID)
	_, err := w.sheetsService.Spreadsheets.Values.Update(w.spreadsheetID, writeRange, &sheets.ValueRange{Values: values}).ValueInputOption(valueInputOption).Context(ctx).Do()

	if err != nil {
		return fmt.Errorf("failed to set headers at '%s'!A1 in spreadsheet '%s': %w", sheetName, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: Headers set successfully in sheet '%s'.", sheetName)
	return nil
}

func (w *GoogleSheetsWriter) AppendRows(ctx context.Context, sheetName string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	appendRange := fmt.Sprintf("'%s'", sheetName)
	valueInputOption := "USER_ENTERED"
	insertDataOption := "INSERT_ROWS"
	log.Printf("API Sheets: Appending %d rows to sheet '%s' in spreadsheet '%s'...", len(rows), sheetName, w.spreadsheetID)
	_, err := w.sheetsService.Spreadsheets.Values.Append(w.spreadsheetID, appendRange, &sheets.ValueRange{Values: rows}).ValueInputOption(valueInputOption).InsertDataOption(insertDataOption).Context(context.Background()).Do()

	if err != nil {
		return fmt.Errorf("failed to append %d rows to sheet '%s' in spreadsheet '%s': %w", len(rows), sheetName, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: %d rows appended successfully to sheet '%s'.", len(rows), sheetName)

	return nil
}

func (w *GoogleSheetsWriter) EnsureSheetExists(ctx context.Context, sheetName string) error {
	log.Printf("API Sheets: Checking if sheet '%s' exists in spreadsheet '%s' (with context)...", sheetName, w.spreadsheetID)
	spreedsheet, err := w.sheetsService.Spreadsheets.Get(w.spreadsheetID).Fields("sheets.properties.title").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("failed to get spreadsheet details for '%s' to check for sheet '%s': %w", w.spreadsheetID, sheetName, err)
	}

	for _, sheet := range spreedsheet.Sheets {
		if sheet.Properties.Title == sheetName {
			log.Printf("API Sheets: Sheet '%s' already exists in spreadsheet '%s'.", sheetName, w.spreadsheetID)
			return nil
		}
	}

	log.Printf("API Sheets: Sheet '%s' doesn't exist in spreadsheet '%s'. Creating...", sheetName, w.spreadsheetID)
	addSheetRequest := &sheets.Request{
		AddSheet: &sheets.AddSheetRequest{
			Properties: &sheets.SheetProperties{
				Title: sheetName,
			},
		},
	}

	batchUpdateRequest := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{addSheetRequest},
	}

	batchUpdateCallFunc := func() error {
		log.Printf("API Sheets: Performing BatchUpdate to create sheet '%s' in spreadsheet '%s' (with context)...", sheetName, w.spreadsheetID)
		_, err := w.sheetsService.Spreadsheets.BatchUpdate(w.spreadsheetID, batchUpdateRequest).Context(ctx).Do()
		return err
	}

	err = w.executeSheetsCall(ctx, batchUpdateCallFunc, fmt.Sprintf("create sheet '%s' via BatchUpdate", sheetName))
	if err != nil {
		return fmt.Errorf("failed to create sheet '%s' in spreadsheet '%s': %w", sheetName, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: Sheet '%s' created successfully in spreadsheet '%s'.", sheetName, w.spreadsheetID)

	return nil
}

func isRetryableSheetsError(err error) bool {
	if err == nil {
		return false
	}

	apiErr, ok := err.(*googleapi.Error)
	if !ok {
		return false
	}

	if apiErr.Code >= 500 && apiErr.Code < 600 {
		log.Printf("Google API 5xx error (%d): %s. Retrying...", apiErr.Code, apiErr.Message)
		return true
	}

	if apiErr.Code == 429 {
		log.Printf("Google API 429 error (Quota Limit). Retrying...")
		return true
	}

	if apiErr.Code == 403 && strings.Contains(strings.ToLower(apiErr.Message), "rateLimitExceeded") {
		log.Printf("Google API 403 error (Forbidden / Quota Exceeded). Retrying...")
		return true
	}

	return false
}

func (w *GoogleSheetsWriter) executeSheetsCall(ctx context.Context, callFunc func() error, operationDesc string) error {
	baseDelay := w.retryDelay
	maxAttempts := w.retryMaxAttempts

	for attempt := 0; attempt <= maxAttempts; attempt++ {

		select {
		case <-ctx.Done():
			log.Printf("Sheets API operation '%s' cancelled via context before attempt %d: %v", operationDesc, attempt+1, ctx.Err())
			return fmt.Errorf("operation '%s' cancelled via context: %w", operationDesc, ctx.Err())
		default:
		}

		err := callFunc()
		if err == nil {
			return nil
		}

		if isRetryableSheetsError(err) && attempt < maxAttempts {
			delay := baseDelay * time.Duration(1<<attempt)

			log.Printf("Sheets API operation '%s' failed (attempt %d/%d): %v. Waiting %s before retrying...", operationDesc, attempt+1, maxAttempts+1, err, delay)

			select {
			case <-time.After(delay):

			case <-ctx.Done():
				log.Printf("Sheets API operation '%s' cancelled via context during wait.", ctx.Err())
				return fmt.Errorf("operation '%s' cancelled via context during retry wait: %w", operationDesc, ctx.Err())
			}
		} else {

			return fmt.Errorf("fatal Sheets API operation '%s' failure after %d attempts: %w", operationDesc, attempt+1, err)
		}
	}

	return fmt.Errorf("executeSheetsCall reached unexpected state for operation: %s", operationDesc)
}
