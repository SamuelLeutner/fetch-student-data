package services

import (
	"context"
	"encoding/base64"
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

func NewGoogleSheetsWriter(ctx context.Context, spreadsheetID string, CredentialsJSONBase64 string, retryMaxAttempts int, retryDelay time.Duration,) (*GoogleSheetsWriter, error) {
	var err error
	var credentialsJSON []byte
	var credSourceDescription string

	envCredsBase64 := os.Getenv("GOOGLE_CREDENTIALS_JSON_BASE64")
	if envCredsBase64 != "" {
		log.Println("INFO: Variável de ambiente GOOGLE_CREDENTIALS_JSON_BASE64 encontrada. Usando-a.")
		credentialsJSON, err = base64.StdEncoding.DecodeString(envCredsBase64)
		if err != nil {
			return nil, fmt.Errorf("falha ao decodificar GOOGLE_CREDENTIALS_JSON_BASE64: %w", err)
		}
		credSourceDescription = "variável de ambiente GOOGLE_CREDENTIALS_JSON_BASE64"
	} else if CredentialsJSONBase64 != "" {
		log.Printf("INFO: GOOGLE_CREDENTIALS_JSON_BASE64 não definida. Tentando arquivo de credenciais: %s", CredentialsJSONBase64)
		credentialsJSON, err = os.ReadFile(CredentialsJSONBase64)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("WARN: Arquivo de credenciais '%s' não encontrado. Tentará Application Default Credentials.", CredentialsJSONBase64)
				credentialsJSON = nil
			} else {
				return nil, fmt.Errorf("falha ao ler arquivo de credenciais '%s': %w", CredentialsJSONBase64, err)
			}
		} else {
			credSourceDescription = fmt.Sprintf("arquivo ('%s')", CredentialsJSONBase64)
		}
	} else {
		log.Println("INFO: Nem GOOGLE_CREDENTIALS_JSON_BASE64 nem CredentialsJSONBase64 fornecidos. Tentando Application Default Credentials.")

		credSourceDescription = "Application Default Credentials"
	}

	var sheetsService *sheets.Service
	if credentialsJSON != nil {
		log.Printf("INFO: Configurando cliente Google Sheets com credenciais JSON de: %s", credSourceDescription)
		config, err := google.JWTConfigFromJSON(credentialsJSON, sheets.SpreadsheetsScope)
		if err != nil {
			return nil, fmt.Errorf("falha ao configurar JWT a partir das credenciais JSON (fonte: %s): %w", credSourceDescription, err)
		}
		client := config.Client(ctx)
		sheetsService, err = sheets.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			return nil, fmt.Errorf("falha ao criar cliente da API Google Sheets usando JWT (fonte: %s): %w", credSourceDescription, err)
		}
	} else {
		log.Println("INFO: Configurando cliente Google Sheets com Application Default Credentials.")
		sheetsService, err = sheets.NewService(ctx)
		if err != nil {
			return nil, fmt.Errorf("falha ao criar cliente da API Google Sheets usando Application Default Credentials: %w. Verifique se ADC estão configuradas se nenhuma credencial explícita foi fornecida.", err)
		}
	}

	log.Println("INFO: Cliente do Google Sheets inicializado com sucesso.")
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
