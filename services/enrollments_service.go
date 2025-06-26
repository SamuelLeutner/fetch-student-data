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

	sheetName := c.determineSheetName(params)
	log.Printf("Sheet name determined: '%s'", sheetName)

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
		return c.Writer.OverwriteSheetData(ctx, sheetName, headers, [][]interface{}{})
	}

	allEnrollments := make([]models.Enrollment, 0, totalElements)
	allEnrollments = append(allEnrollments, firstPageElements...)

	if totalPages > 1 {
		remainingPages := totalPages - 1
		batchSize := c.Config.MaxPagesPerBatch
		if remainingPages < batchSize {
			batchSize = remainingPages
		}

		currentPage := 1
		for currentPage < totalPages {
			select {
			case <-ctx.Done():
				log.Printf("Process cancelled via context before starting batch from page %d: %v", currentPage, ctx.Err())
				return fmt.Errorf("filtered enrollment fetch cancelled: %w", ctx.Err())
			default:
			}
			
			batchData, err := c.processBatchEnrollmentsFiltered(ctx, currentPage, batchSize, fetchParams)
			if err != nil {
				log.Printf("Failed to process batch of pages %d-%d: %v. Moving to next batch.", currentPage, currentPage+batchSize-1, err)
			} else {
				allEnrollments = append(allEnrollments, batchData...)
			}
			currentPage += batchSize
			c.logProgress(startTime, currentPage, totalPages, len(allEnrollments))
		}
	}

	log.Printf("All %d enrollments fetched. Writing to sheet '%s'...", len(allEnrollments), sheetName)
	if err := c.writeAllEnrollmentsToSheet(ctx, allEnrollments, sheetName, headers); err != nil {
		return fmt.Errorf("failed to write all enrollments to sheet: %w", err)
	}

	log.Printf("Process completed! Total: %d enrollments written to sheet '%s'.", len(allEnrollments), sheetName)
	return nil
}

func (c *JacadClient) writeAllEnrollmentsToSheet(ctx context.Context, data []models.Enrollment, sheetName string, headers []string) error {
	rows := make([][]interface{}, len(data))
	for i, item := range data {
		rows[i] = make([]interface{}, len(headers))
		for j, field := range headers {
			switch field {
			case "idMatricula":
				rows[i][j] = item.IdMatricula
			case "aluno":
				rows[i][j] = utils.GetStringOrEmpty(item.Aluno)
			case "ra":
				rows[i][j] = utils.GetStringOrEmpty(item.RA)
			case "curso":
				rows[i][j] = utils.GetStringOrEmpty(item.Curso)
			case "turma":
				rows[i][j] = utils.GetStringOrEmpty(item.Turma)
			case "status":
				rows[i][j] = utils.GetStringOrEmpty(item.Status)
			case "periodoLetivo":
				rows[i][j] = utils.GetStringOrEmpty(item.PeriodoLetivo)
			case "unidadeFisica":
				rows[i][j] = utils.GetStringOrEmpty(item.UnidadeFisica)
			case "organizacao":
				rows[i][j] = utils.GetStringOrEmpty(item.Organizacao)
			case "idOrg":
				rows[i][j] = item.OrgID
			case "dataMatricula":
				rows[i][j] = utils.GetTimeOrNilDate(item.DataMatricula)
			case "dataAtivacao":
				rows[i][j] = utils.GetTimeOrNilDate(item.DataAtivacao)
			case "dataCadastro":
				rows[i][j] = utils.GetTimeOrNilDate(item.DataCadastro)
			default:
				rows[i][j] = ""
			}
		}
	}

	return c.Writer.OverwriteSheetData(ctx, sheetName, headers, rows)
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


func (c *JacadClient) determineSheetName(params *requests.FetchEnrollmentsRequest) string {
	orgName := config.GetOrganizationNameByID(params.OrgId)
	if orgName == "" {
		orgName = config.AppConfig.DefaultOrgSheet
	}
	return fmt.Sprintf("Matrículas %s STATUS: %s | Período ID %d", orgName, params.StatusMatricula, params.IdPeriodoLetivo)
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
