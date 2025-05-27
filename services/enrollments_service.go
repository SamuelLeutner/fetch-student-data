package services

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	requests "github.com/SamuelLeutner/fetch-student-data/api/Requests"
	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/models"
	"github.com/SamuelLeutner/fetch-student-data/utils"
)

func (c *JacadClient) FetchEnrollmentsFiltered(ctx context.Context, params *requests.FetchEnrollmentsRequest) error {
	log.Printf("Starting filtered enrollment fetch for PeriodoLetivo='%d', StatusMatricula='%s' (with context)...", params.IdPeriodoLetivo, params.StatusMatricula)
	startTime := time.Now()

	headers := []string{
		"idMatricula", "aluno", "ra", "curso",
		"turma", "status", "periodoLetivo",
		"unidadeFisica", "organizacao",
		"idOrg", "dataMatricula",
		"dataAtivacao", "dataCadastro",
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
	var mu sync.Mutex
	wg := sync.WaitGroup{}
	var allData []models.Enrollment

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
			case "idMatricula":
				row[i] = item.IdMatricula
			case "aluno":
				row[i] = utils.GetStringOrEmpty(item.Aluno)
			case "ra":
				row[i] = utils.GetStringOrEmpty(item.RA)
			case "curso":
				row[i] = utils.GetStringOrEmpty(item.Curso)
			case "turma":
				row[i] = utils.GetStringOrEmpty(item.Turma)
			case "status":
				row[i] = utils.GetStringOrEmpty(item.Status)
			case "periodoLetivo":
				row[i] = utils.GetStringOrEmpty(item.PeriodoLetivo)
			case "unidadeFisica":
				row[i] = utils.GetStringOrEmpty(item.UnidadeFisica)
			case "organizacao":
				row[i] = utils.GetStringOrEmpty(item.Organizacao)
			case "idOrg":
				row[i] = item.OrgID
			case "dataMatricula":
				row[i] = utils.GetTimeOrNilDate(item.DataMatricula)
			case "dataAtivacao":
				row[i] = utils.GetTimeOrNilDate(item.DataAtivacao)
			case "dataCadastro":
				row[i] = utils.GetTimeOrNilDate(item.DataCadastro)
			default:
				row[i] = ""
			}
		}
		buffersBySheetName[sheetName] = append(buffersBySheetName[sheetName], row)
		orgCounts[sheetName]++
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
	log.Printf("Starting setupEnrollmentSheets for OrgID: %d, AcademicPeriodID: %d, EnrollmentStatus: %s", params.OrgId, params.IdPeriodoLetivo, params.StatusMatricula)
	orgName := config.GetOrganizationNameByID(params.OrgId)

	log.Println("Fetching academic period name asynchronously...")
	var sheetName string
	if orgName == "" {
		sheetName = fmt.Sprintf("%s %s | Período ID %d", config.AppConfig.DefaultOrgSheet, params.StatusMatricula, params.IdPeriodoLetivo)
	}

	sheetName = fmt.Sprintf("Matrículas %s STATUS: %s | Período ID %d", orgName, params.StatusMatricula, params.IdPeriodoLetivo)

	log.Printf("Sheet name set to: '%s'", sheetName)

	if err := c.Writer.EnsureSheetExists(ctx, sheetName); err != nil {
		if ctx.Err() != nil {
			log.Printf("EnsureSheetExists for '%s' cancelled via context: %v", sheetName, ctx.Err())
			return "", fmt.Errorf("sheet setup cancelled (ensure sheet): %w", ctx.Err())
		}
		return "", fmt.Errorf("failed to ensure sheet '%s' exists: %w", sheetName, err)
	}

	select {
	case <-ctx.Done():
		log.Printf("Context cancelled after EnsureSheetExists for '%s': %v", sheetName, ctx.Err())
		return "", fmt.Errorf("sheet setup cancelled: %w", ctx.Err())
	default:
	}

	if err := c.Writer.Clear(ctx, sheetName); err != nil {
		if ctx.Err() != nil {
			log.Printf("Clear for '%s' cancelled via context: %v", sheetName, ctx.Err())
			return "", fmt.Errorf("sheet setup cancelled (clear sheet): %w", ctx.Err())
		}
		return "", fmt.Errorf("failed to clear sheet '%s': %w", sheetName, err)
	}
	select {
	case <-ctx.Done():
		log.Printf("Context cancelled after Clear for '%s': %v", sheetName, ctx.Err())
		return "", fmt.Errorf("sheet setup cancelled: %w", ctx.Err())
	default:
	}

	if err := c.Writer.SetHeaders(ctx, sheetName, headers); err != nil {
		if ctx.Err() != nil {
			log.Printf("SetHeaders for '%s' cancelled via context: %v", sheetName, ctx.Err())
			return "", fmt.Errorf("sheet setup cancelled (set headers): %w", ctx.Err())
		}
		return "", fmt.Errorf("failed to set headers in sheet '%s': %w", sheetName, err)
	}

	log.Printf("Sheet '%s' verified/created and configured successfully.", sheetName)
	return sheetName, nil
}
