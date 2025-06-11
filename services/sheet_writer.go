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

func NewGoogleSheetsWriter(ctx context.Context, spreadsheetID string, CredentialsJSONBase64 string, retryMaxAttempts int, retryDelay time.Duration) (*GoogleSheetsWriter, error) {
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

func (w *GoogleSheetsWriter) AppendRows(ctx context.Context, sheetName string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	appendRange := fmt.Sprintf("'%s'", sheetName)
	valueInputOption := "USER_ENTERED"
	insertDataOption := "INSERT_ROWS"

	appendCallFunc := func() error {
		log.Printf("API Sheets: Anexando %d linhas na aba '%s'...", len(rows), sheetName)
		_, err := w.sheetsService.Spreadsheets.Values.Append(w.spreadsheetID, appendRange, &sheets.ValueRange{Values: rows}).
			ValueInputOption(valueInputOption).
			InsertDataOption(insertDataOption).
			Context(ctx).
			Do()
		return err
	}

	err := w.executeSheetsCall(ctx, appendCallFunc, fmt.Sprintf("anexar linhas na aba '%s'", sheetName))
	if err != nil {
		return fmt.Errorf("falha ao anexar %d linhas na aba '%s': %w", len(rows), sheetName, err)
	}
	return nil
}

func (w *GoogleSheetsWriter) OverwriteSheetData(ctx context.Context, sheetName string, headers []string, rows [][]interface{}) error {
	if err := w.EnsureSheetExists(ctx, sheetName); err != nil {
		return err
	}

	if err := w.Clear(ctx, sheetName); err != nil {
		return err
	}

	allData := make([][]interface{}, 0, 1+len(rows))
	if len(headers) > 0 {
		headerRow := make([]interface{}, len(headers))
		for i, h := range headers {
			headerRow[i] = h
		}
		allData = append(allData, headerRow)
	}
	allData = append(allData, rows...)

	if len(allData) == 0 {
		log.Printf("INFO: Nenhum dado (cabeçalhos ou linhas) para escrever na aba '%s'.", sheetName)
		return nil
	}

	writeRange := fmt.Sprintf("'%s'!A1", sheetName)
	updateReq := &sheets.ValueRange{Values: allData}

	updateCallFunc := func() error {
		log.Printf("API Sheets: Escrevendo %d linhas totais (cabeçalhos + dados) na aba '%s'...", len(allData), sheetName)
		_, err := w.sheetsService.Spreadsheets.Values.Update(w.spreadsheetID, writeRange, updateReq).
			ValueInputOption("USER_ENTERED").
			Context(ctx).
			Do()
		return err
	}

	err := w.executeSheetsCall(ctx, updateCallFunc, fmt.Sprintf("escrever dados na aba '%s'", sheetName))
	if err != nil {
		return fmt.Errorf("falha ao escrever dados na aba '%s': %w", sheetName, err)
	}

	log.Printf("API Sheets: Aba '%s' sobrescrita com sucesso com %d linhas totais.", sheetName, len(allData))
	return nil
}

func (w *GoogleSheetsWriter) Clear(ctx context.Context, sheetName string) error {
	clearRange := fmt.Sprintf("'%s'", sheetName)
	req := sheets.ClearValuesRequest{}

	clearCallFunc := func() error {
		log.Printf("API Sheets: Limpando a aba '%s' na planilha '%s'...", sheetName, w.spreadsheetID)
		_, err := w.sheetsService.Spreadsheets.Values.Clear(w.spreadsheetID, clearRange, &req).Context(ctx).Do()
		return err
	}

	err := w.executeSheetsCall(ctx, clearCallFunc, fmt.Sprintf("limpar aba '%s'", sheetName))
	if err != nil {
		return fmt.Errorf("falha ao limpar a aba '%s' na planilha '%s': %w", sheetName, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: Aba '%s' limpa com sucesso.", sheetName)
	return nil
}

func (w *GoogleSheetsWriter) SetHeaders(ctx context.Context, sheetName string, headers []string) error {
	writeRange := fmt.Sprintf("'%s'!A1", sheetName)
	var values [][]interface{}
	var headerInterfaces []interface{}
	for _, h := range headers {
		headerInterfaces = append(headerInterfaces, h)
	}
	values = append(values, headerInterfaces)

	updateReq := &sheets.ValueRange{Values: values}
	updateCallFunc := func() error {
		log.Printf("API Sheets: Definindo cabeçalhos em '%s'!A1 na planilha '%s'...", sheetName, w.spreadsheetID)
		_, err := w.sheetsService.Spreadsheets.Values.Update(w.spreadsheetID, writeRange, updateReq).
			ValueInputOption("USER_ENTERED").
			Context(ctx).
			Do()
		return err
	}

	err := w.executeSheetsCall(ctx, updateCallFunc, fmt.Sprintf("definir cabeçalhos na aba '%s'", sheetName))
	if err != nil {
		return fmt.Errorf("falha ao definir cabeçalhos em '%s'!A1: %w", sheetName, err)
	}

	log.Printf("API Sheets: Cabeçalhos definidos com sucesso na aba '%s'.", sheetName)
	return nil
}

func (w *GoogleSheetsWriter) EnsureSheetExists(ctx context.Context, sheetName string) error {
	log.Printf("API Sheets: Verificando se a aba '%s' existe na planilha '%s'...", sheetName, w.spreadsheetID)
	spreadsheet, err := w.sheetsService.Spreadsheets.Get(w.spreadsheetID).Fields("sheets.properties.title").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("falha ao obter detalhes da planilha '%s' para verificar a aba '%s': %w", w.spreadsheetID, sheetName, err)
	}

	for _, sheet := range spreadsheet.Sheets {
		if sheet.Properties.Title == sheetName {
			log.Printf("API Sheets: A aba '%s' já existe na planilha '%s'.", sheetName, w.spreadsheetID)
			return nil
		}
	}

	log.Printf("API Sheets: A aba '%s' não existe na planilha '%s'. Criando...", sheetName, w.spreadsheetID)
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
		log.Printf("API Sheets: Executando BatchUpdate para criar a aba '%s'...", sheetName)
		_, err := w.sheetsService.Spreadsheets.BatchUpdate(w.spreadsheetID, batchUpdateRequest).Context(ctx).Do()
		return err
	}

	err = w.executeSheetsCall(ctx, batchUpdateCallFunc, fmt.Sprintf("criar aba '%s'", sheetName))
	if err != nil {
		return fmt.Errorf("falha ao criar a aba '%s' na planilha '%s': %w", sheetName, w.spreadsheetID, err)
	}

	log.Printf("API Sheets: Aba '%s' criada com sucesso.", sheetName)
	return nil
}

func (w *GoogleSheetsWriter) executeSheetsCall(ctx context.Context, callFunc func() error, operationDesc string) error {
	baseDelay := w.retryDelay
	maxAttempts := w.retryMaxAttempts

	for attempt := 0; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			log.Printf("Operação da API Sheets '%s' cancelada via contexto antes da tentativa %d: %v", operationDesc, attempt+1, ctx.Err())
			return fmt.Errorf("operação '%s' cancelada via contexto: %w", operationDesc, ctx.Err())
		default:
		}

		err := callFunc()
		if err == nil {
			return nil
		}

		if isRetryableSheetsError(err) && attempt < maxAttempts {
			delay := baseDelay * time.Duration(1<<attempt)
			log.Printf("Operação da API Sheets '%s' falhou (tentativa %d/%d): %v. Aguardando %s antes de tentar novamente...", operationDesc, attempt+1, maxAttempts+1, err, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				log.Printf("Operação da API Sheets '%s' cancelada via contexto durante a espera.", operationDesc)
				return fmt.Errorf("operação '%s' cancelada via contexto durante a espera da nova tentativa: %w", operationDesc, ctx.Err())
			}
		} else {
			return fmt.Errorf("falha fatal na operação da API Sheets '%s' após %d tentativas: %w", operationDesc, attempt+1, err)
		}
	}
	return fmt.Errorf("executeSheetsCall atingiu um estado inesperado para a operação: %s", operationDesc)
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
		log.Printf("Google API 5xx error (%d): %s. Tentando novamente...", apiErr.Code, apiErr.Message)
		return true
	}
	if apiErr.Code == 429 {
		log.Printf("Google API 429 error (Resource Exhausted / Quota Limit). Tentando novamente...")
		return true
	}
	if apiErr.Code == 403 && strings.Contains(strings.ToLower(apiErr.Message), "ratelimitexceeded") {
		log.Printf("Google API 403 error (Rate Limit Exceeded). Tentando novamente...")
		return true
	}
	return false
}
