package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/models"
)

func (c *JacadClient) GetPeriodoNameByID(ctx context.Context, idOrg int, IDPeriodoLetivo int) (string, bool) {
	for _, status := range config.AppConfig.EditalStatus {
		fetchParams := make(map[string]string)
		fetchParams["idOrg"] = strconv.Itoa(idOrg)
		fetchParams["idPeriodoLetivo"] = strconv.Itoa(IDPeriodoLetivo)
		fetchParams["pageSize"] = strconv.Itoa(config.AppConfig.PageSize)
		fetchParams["statusEdital"] = status

		periodoElements, err := c.FetchPeriod(ctx, fetchParams)
		if err != nil {
			log.Printf("Error fetching period for status %s: %v", status, err)
			return "", false
		}

		if len(periodoElements) == 0 {
			log.Printf("No periods found for ID %d and status %s. Skipping.", idOrg, status)
			continue
		}

		for _, period := range periodoElements {
			if period.IDPeriodoLetivo == IDPeriodoLetivo {
				log.Printf(
					"Found matching period for ID %d (Status: '%s'): Descricao='%s', PeriodoLetivo='%s'",
					IDPeriodoLetivo, status, period.Descricao, period.PeriodoLetivo,
				)
				return period.PeriodoLetivo, true
			}
		}

		log.Printf("No period matching ID %d found within elements for status '%s'.", IDPeriodoLetivo, status)
	}

	log.Printf("No period matching ID %d found across all configured statuses.", IDPeriodoLetivo)
	return "", false
}

func (c *JacadClient) FetchPeriod(ctx context.Context, params map[string]string) ([]models.Period, error) {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, v)
	}

	endpoint := c.Config.Endpoints["PROCESS_NOTICES"]
	url := fmt.Sprintf("%s%s?%s", c.Config.APIBase, endpoint, q.Encode())

	log.Printf("Fetching period from %s", url)

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
