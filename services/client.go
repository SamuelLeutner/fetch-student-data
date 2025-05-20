package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	requests "github.com/SamuelLeutner/fetch-student-data/api/Requests"
	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/models"
	"github.com/SamuelLeutner/fetch-student-data/utils"
)

type SheetWriter interface {
	EnsureSheetExists(ctx context.Context, sheetName string) error
	Clear(ctx context.Context, sheetName string) error
	SetHeaders(ctx context.Context, sheetName string, headers []string) error
	AppendRows(ctx context.Context, sheetName string, rows [][]interface{}) error
}

type JacadClient struct {
	Config      *config.Config
	Client      *http.Client
	Writer      SheetWriter
	token       string
	tokenExpiry time.Time
	muAuth      sync.Mutex
}

func NewJacadClient(config *config.Config, writer SheetWriter) *JacadClient {
	return &JacadClient{
		Config: config,
		Client: &http.Client{Timeout: 60 * time.Second},
		Writer: writer,
	}
}

func (c *JacadClient) MakeRequest(ctx context.Context, method, url string, headers map[string]string, body io.Reader) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= c.Config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			log.Printf("Request '%s %s' cancelled via context before attempt %d: %v", method, strings.Split(url, "?")[0], attempt+1, ctx.Err())
			return nil, fmt.Errorf("request '%s %s' cancelled via context: %w", method, strings.Split(url, "?")[0], ctx.Err())
		default:
		}

		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, fmt.Errorf("error creating request on attempt %d: %w", attempt+1, err)
		}

		if headers != nil {
			for key, value := range headers {
				req.Header.Set(key, value)
			}
		}

		log.Printf("Request (%s): %s (Attempt %d/%d)...", method, strings.Split(url, "?")[0], attempt+1, c.Config.MaxRetries+1)

		resp, err := c.Client.Do(req)

		if err != nil {
			lastErr = fmt.Errorf("http client error on attempt %d: %w", attempt+1, err)
		} else if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
			} else {
				lastErr = fmt.Errorf("HTTP %d: Error reading body: %w", resp.StatusCode, readErr)
			}
		} else if resp.StatusCode == http.StatusUnauthorized {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("HTTP %d: error reading error response body: %w", resp.StatusCode, readErr)
			}
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		} else if resp.StatusCode >= 400 {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("HTTP %d: error reading error response body: %w", resp.StatusCode, readErr)
			}
			log.Printf("HTTP %d error: %s", resp.StatusCode, string(bodyBytes))
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		} else {
			defer resp.Body.Close()
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("error reading response body on success: %w", err)
			}
			return bodyBytes, nil
		}

		if attempt < c.Config.MaxRetries {
			delay := c.Config.RetryDelay * time.Duration(1<<attempt)
			log.Printf("Request failed (attempt %d/%d): %v. Waiting %s before retrying...", attempt+1, c.Config.MaxRetries+1, lastErr, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				log.Printf("Context cancelled during retry wait for %s: %v", url, ctx.Err())
				return nil, fmt.Errorf("request cancelled during retry wait after %d attempts for %s: %w", attempt+1, url, ctx.Err())
			}
		} else {
			break
		}
	}
	return nil, fmt.Errorf("request failed after %d attempts: %w", c.Config.MaxRetries+1, lastErr)
}

func (c *JacadClient) FetchPage(ctx context.Context, endpoint string, page, pageSize int, params map[string]string) ([]models.Enrollment, *models.Page, error) {
	q := url.Values{}
	q.Set("currentPage", fmt.Sprintf("%d", page))
	q.Set("pageSize", fmt.Sprintf("%d", pageSize))
	for k, v := range params {
		q.Set(k, v)
	}

	url := fmt.Sprintf("%s%s?%s", c.Config.APIBase, endpoint, q.Encode())

	token, err := c.GetAuthToken(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("failed to get token for page %d due to context cancellation: %w", page, ctx.Err())
		}
		return nil, nil, fmt.Errorf("failed to get token for page %d: %w", page, err)
	}

	headers := map[string]string{
		"Authorization": "Bearer " + token,
		"Content-Type":  "application/json",
	}

	body, err := c.MakeRequest(ctx, http.MethodGet, url, headers, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("fetching page %d cancelled via context: %w", page, ctx.Err())
		}
		return nil, nil, fmt.Errorf("error fetching page %d from %s: %w", page, endpoint, err)
	}

	var apiResp models.APIResponse[models.Enrollment]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, nil, fmt.Errorf("error parsing API response from page %d: %w", page, err)
	}

	return apiResp.Elements, apiResp.Page, nil
}

func (c *JacadClient) FetchEnrollmentsFiltered(ctx context.Context, params *requests.FetchEnrollmentsRequest) error {
	log.Printf("Starting filtered enrollment fetch for PeriodoLetivo='%d', StatusMatricula='%s' (with context)...", params.IdPeriodoLetivo, params.StatusMatricula)
	startTime := time.Now()

	headers := []string{
		"id", "student", "ra", "course", "class", "status",
		"academicTerm", "physicalUnit", "organization", "orgID",
		"enrollDate", "activateDate", "createdDate",
	}

	fetchParams := make(map[string]string)
	if params.IdPeriodoLetivo != 0 {
		fetchParams["idPeriodoLetivo"] = strconv.Itoa(params.IdPeriodoLetivo)
	}
	if params.StatusMatricula != "" {
		fetchParams["statusMatricula"] = params.StatusMatricula
	}

	sheetName, err := c.setupEnrollmentSheets(ctx, headers, params)
	if err != nil {
		return fmt.Errorf("failed to setup enrollment sheets: %w", err)
	}

	log.Println("Fetching initial page (0) to get total pages...")
	firstPageElements, Page, err := c.FetchPage(ctx, c.Config.Endpoints["ENROLLMENTS"], 0, c.Config.PageSize, fetchParams)

	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("fetching initial page cancelled: %w", ctx.Err())
		}

		return fmt.Errorf("failed to fetch initial page to get total: %w", err)
	}

	if Page == nil {
		return fmt.Errorf("API response for page 0 did not contain pagination info")
	}

	totalPages := Page.TotalPages
	totalElements := Page.TotalElements

	log.Printf("Initial page fetched. Total pages: %d (Total elements: %d)", totalPages, totalElements)

	if totalPages == 0 || totalElements == 0 {
		log.Println("Total pages or elements is zero. No enrollments to process.")

		return nil
	}

	log.Printf("Writing %d enrollments from the initial page (0) to sheets", len(firstPageElements))
	if writeErr := c.writeEnrollmentsToSheets(ctx, firstPageElements, sheetName, headers); writeErr != nil {
		log.Printf("Error writing initial page data: %v", writeErr)

	}
	totalProcessed := len(firstPageElements)
	currentPage := 1

	for currentPage < totalPages {
		select {
		case <-ctx.Done():
			log.Printf("Process cancelled via context before starting batch from page %d: %v", currentPage, ctx.Err())
			log.Printf("Process cancelled. Total enrollments processed before cancellation: %d", totalProcessed)
			return fmt.Errorf("filtered enrollment fetch cancelled: %w", ctx.Err())
		default:
		}

		remainingPages := totalPages - currentPage
		batchSize := c.Config.MaxPagesPerBatch
		if remainingPages < batchSize {
			batchSize = remainingPages
		}

		log.Printf("Processing batch: pages %d to %d (batch size: %d) (with context and filters)...", currentPage, currentPage+batchSize-1, batchSize)

		batchData, err := c.processBatchEnrollmentsFiltered(ctx, currentPage, batchSize, fetchParams)

		if err != nil {
			log.Printf("Failed to process batch of pages %d-%d: %v. Moving to next batch.", currentPage, currentPage+batchSize-1, err)

			currentPage += batchSize
		} else {
			if len(batchData) > 0 {
				log.Printf("Writing %d enrollments from batch to sheets (with context)...", len(batchData))

				if writeErr := c.writeEnrollmentsToSheets(ctx, batchData, sheetName, headers); writeErr != nil {
					log.Printf("Error writing batch data: %v", writeErr)
				}
			}
			totalProcessed += len(batchData)
			currentPage += batchSize
		}

		c.logProgress(startTime, currentPage, totalPages, totalProcessed)

		select {
		case <-ctx.Done():
			log.Printf("Process cancelled via context during sleep: %v", ctx.Err())
			log.Printf("Process cancelled. Total enrollments processed before cancellation: %d", totalProcessed)
			return fmt.Errorf("filtered enrollment fetch cancelled during sleep: %w", ctx.Err())
		}
	}

	log.Printf("Process completed! Total: %d enrollments", totalProcessed)
	return nil
}

func (c *JacadClient) logProgress(startTime time.Time, currentPage, totalPages, totalProcessed int) {
	elapsed := time.Since(startTime).Seconds()
	progress := 0.0

	if totalPages > 0 {

		progress = float64(currentPage) / float64(totalPages) * 100
	}

	log.Printf("Pages (batches started): %d/%d (%.1f%%) | Enrollments Processed: %d | Time: %.1fs",
		currentPage, totalPages, progress, totalProcessed, elapsed)
}

func (c *JacadClient) processBatchEnrollmentsFiltered(ctx context.Context, startPage, count int, params map[string]string) ([]models.Enrollment, error) {
	var allData []models.Enrollment
	var mu sync.Mutex

	wg := sync.WaitGroup{}

	dataChan := make(chan []models.Enrollment, count)
	errorCount := 0

	log.Printf("Starting concurrent fetch of %d pages (batch %d-%d) (Max Concurrency: %d)...", count, startPage, startPage+count-1, c.Config.MaxParallelRequests)

	pagesToFetch := make(chan int, count)
	for i := 0; i < count; i++ {
		pagesToFetch <- startPage + i
	}
	close(pagesToFetch)

	maxWorkers := c.Config.MaxParallelRequests
	if count < maxWorkers {
		maxWorkers = count
	}

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for pageNum := range pagesToFetch {
				select {
				case <-ctx.Done():
					log.Printf("Worker stopping due to context cancellation for page %d: %v", pageNum, ctx.Err())
					return
				default:

				}

				log.Printf("-> Fetching page %d (batch %d-%d) (with context and filters)...", pageNum, startPage, startPage+count-1)

				pageElements, _, err := c.FetchPage(ctx, c.Config.Endpoints["ENROLLMENTS"], pageNum, c.Config.PageSize, params)

				if err != nil {
					if ctx.Err() != nil {
						log.Printf("Failed to fetch page %d due to context cancellation: %v", pageNum, err)
					} else {
						log.Printf("Failed to fetch page %d after retries: %v", pageNum, err)
						mu.Lock()
						errorCount++
						mu.Unlock()
					}
					continue
				}

				select {
				case dataChan <- pageElements:
					if len(pageElements) > 0 {
						log.Printf("<- Page %d (batch %d-%d): %d enrollments found.", pageNum, startPage, startPage+count-1, len(pageElements))
					} else {
						log.Printf("<- Page %d (batch %d-%d): 0 enrollments found.", pageNum, startPage, startPage+count-1)
					}
				case <-ctx.Done():
					log.Printf("Context cancelled while trying to send data for page %d to channel: %v", pageNum, ctx.Err())
					return
				}
			}
		}()
	}

	wg.Wait()
	close(dataChan)

	for pageData := range dataChan {
		if len(pageData) > 0 {
			mu.Lock()
			allData = append(allData, pageData...)
			mu.Unlock()
		}
	}

	if ctx.Err() != nil {
		log.Printf("Batch processing cancelled via context after waiting for goroutines: %v", ctx.Err())
		return nil, fmt.Errorf("batch processing cancelled: %w", ctx.Err())
	}

	if errorCount > 0 {
		if errorCount == count && count > 0 {
			log.Printf("Batch completed. ALL %d requests in batch failed (not cancelled).", count)
			return nil, fmt.Errorf("all %d requests in batch failed in batch %d-%d", count, startPage, startPage+count-1)
		}
		log.Printf("Batch completed. Total %d enrollments collected from successful requests (%d failures) in batch %d-%d", len(allData), errorCount, startPage, startPage+count-1)

	} else {
		log.Printf("Batch completed. Total %d enrollments collected from successful requests (0 failures) in batch %d-%d", len(allData), startPage, startPage+count-1)
	}

	return allData, nil
}

func (c *JacadClient) writeEnrollmentsToSheets(ctx context.Context, data []models.Enrollment, sheetName string, headers []string) error {
	if len(data) == 0 {
		log.Println("No enrollments to write.")
		return nil
	}
	log.Printf("Organizing %d enrollments by organization for writing...", len(data))

	buffersBySheetName := make(map[string][][]interface{})
	orgCounts := make(map[string]int)

	for _, item := range data {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled during data organization for writing: %v", ctx.Err())
			return fmt.Errorf("data organization cancelled: %w", ctx.Err())
		default:
		}

		if _, exists := buffersBySheetName[sheetName]; !exists {
			buffersBySheetName[sheetName] = make([][]interface{}, 0)
		}
		if _, exists := orgCounts[sheetName]; !exists {
			orgCounts[sheetName] = 0
		}
		row := make([]interface{}, len(headers))
		for i, field := range headers {
			switch field {
			case "id":
				row[i] = item.IdMatricula
			case "student":
				row[i] = utils.GetStringOrEmpty(item.Aluno)
			case "ra":
				row[i] = utils.GetStringOrEmpty(item.RA)
			case "course":
				row[i] = utils.GetStringOrEmpty(item.Curso)
			case "class":
				row[i] = utils.GetStringOrEmpty(item.Turma)
			case "status":
				row[i] = utils.GetStringOrEmpty(item.Status)
			case "academicTerm":
				row[i] = utils.GetStringOrEmpty(item.PeriodoLetivo)
			case "physicalUnit":
				row[i] = utils.GetStringOrEmpty(item.UnidadeFisica)
			case "organization":
				row[i] = utils.GetStringOrEmpty(item.Organizacao)
			case "orgID":
				row[i] = item.OrgID
			case "enrollDate":
				row[i] = utils.GetTimeOrNilDate(item.DataMatricula)
			case "activateDate":
				row[i] = utils.GetTimeOrNilDate(item.DataAtivacao)
			case "createdDate":
				row[i] = utils.GetTimeOrNilDate(item.DataCadastro)
			default:
				row[i] = ""
			}
		}
		buffersBySheetName[sheetName] = append(buffersBySheetName[sheetName], row)
		orgCounts[sheetName]++
	}

	for sheetName, count := range orgCounts {
		log.Printf("%d enrollments for sheet '%s' (ready to write)", count, sheetName)
	}

	for sheetName, rows := range buffersBySheetName {
		if len(rows) > 0 {
			select {
			case <-ctx.Done():
				log.Printf("Context cancelled before writing to sheet '%s': %v", sheetName, ctx.Err())
				return fmt.Errorf("writing to sheet '%s' cancelled: %w", sheetName, ctx.Err())
			default:
			}
			log.Printf("Writing %d rows to sheet '%s' (with context)...", len(rows), sheetName)
			if err := c.Writer.AppendRows(ctx, sheetName, rows); err != nil {
				log.Printf("Error writing data to sheet '%s': %v", sheetName, err)
			} else {
				log.Printf("Writing to sheet '%s' completed.", sheetName)
			}
		}
	}
	log.Println("All batch data processed for writing")
	return nil
}

func (c *JacadClient) setupEnrollmentSheets(ctx context.Context, headers []string, params *requests.FetchEnrollmentsRequest) (string, error) {
	var sheetName string
	orgName, found := config.GetOrganizationNameByID(params.OrgId)

	// TODO: Get periodoLetivo name by ID and put in the sheet name
	periodoLetivoName, found := c.GetPeriodoNameByID(ctx, params.IdPeriodoLetivo)
	if found {
		sheetName = fmt.Sprintf("Matrículas %s %s | %d", orgName, params.StatusMatricula, periodoLetivoName)
	} else {
		sheetName = fmt.Sprintf("Matrículas %s %s | Id Periodo Letivo %d", config.AppConfig.DefaultOrgSheet, params.StatusMatricula, params.IdPeriodoLetivo)
		log.Printf("Aviso: Organização com ID %d não encontrada. Usando nome de planilha padrão: '%s'.", periodoLetivoName, config.AppConfig.DefaultOrgSheet)
	}

	fmt.Println("Organization Name:", orgName)
	fmt.Println("Found:", found)
	fmt.Println("Sheet Name:", sheetName)
	os.Exit(0)

	select {
	case <-ctx.Done():
		log.Printf("Context cancelled during sheet setup for '%s': %v", sheetName, ctx.Err())
		return "", fmt.Errorf("sheet setup cancelled: %w", ctx.Err())
	default:
	}

	err := c.Writer.EnsureSheetExists(ctx, sheetName)
	if err != nil {
		return "", fmt.Errorf("failed to ensure sheet '%s' exists: %w", sheetName, err)
	}

	if err := c.Writer.Clear(ctx, sheetName); err != nil {
		return "", fmt.Errorf("failed to clear sheet '%s': %w", sheetName, err)
	}

	if err := c.Writer.SetHeaders(ctx, sheetName, headers); err != nil {
		return "", fmt.Errorf("failed to set headers in sheet '%s': %w", sheetName, err)
	}
	log.Printf("All required sheets verified/created and configured.")
	return sheetName, nil
}

// TODO: Implement this function to fetch the name of the period by ID with assyncronous requests
func (c *JacadClient) GetPeriodoNameByID(ctx context.Context, idPeriodoLetivo int) ([]models.Period, bool) {
	endpoint := c.Config.Endpoints["PROCESS_NOTICES"]
	pageSize := config.AppConfig.PageSize

	fetchParams := make(map[string]string)
	fetchParams["idOrg"] = strconv.Itoa(idPeriodoLetivo)

	// TODO: Think a for to make to reaquest for the both Status
	// fetchParams["statusEdital"] = config.AppConfig.EditalStatus

	periodo, err := c.FetchPeriod(ctx, endpoint, pageSize, fetchParams)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false
		}

		return nil, false
	}

	return periodo, true
}

func (c *JacadClient) FetchPeriod(ctx context.Context, endpoint string, pageSize int, params map[string]string) ([]models.Period, error) {
	q := url.Values{}
	q.Set("pageSize", fmt.Sprintf("%d", pageSize))
	for k, v := range params {
		q.Set(k, v)
	}

	url := fmt.Sprintf("%s%s?%s", c.Config.APIBase, endpoint, q.Encode())

	token, err := c.GetAuthToken(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("failed to get token due to context cancellation: %w", ctx.Err())
		}
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	headers := map[string]string{
		"Authorization": "Bearer " + token,
		"Content-Type":  "application/json",
	}

	body, err := c.MakeRequest(ctx, http.MethodGet, url, headers, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("fetching period cancelled via context: %w", ctx.Err())
		}
		return nil, fmt.Errorf("error fetching from %s: %w", endpoint, err)
	}

	var apiResp models.APIResponse[models.Period]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("error parsing API response. Error: %w", err)
	}

	return apiResp.Elements, nil
}
