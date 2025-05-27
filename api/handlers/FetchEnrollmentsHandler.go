package handlers

import (
	"context"
	"log"
	"time"

	requests "github.com/SamuelLeutner/fetch-student-data/api/Requests"
	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/services"
	"github.com/gofiber/fiber/v3"
)

func CreateFetchEnrollmentsHandler(client *services.JacadClient, appConfig *config.Config) fiber.Handler {
	return func(c fiber.Ctx) error {
		params := new(requests.FetchEnrollmentsRequest)

		if err := c.Bind().Query(params); err != nil {
			log.Printf("Handler: Error parsing request body: %v", err)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Invalid query params",
				"details": err.Error(),
			})
		}

		ctx, cancel := context.WithTimeout(c.Context(), 10*time.Minute)
		defer cancel()

		log.Printf("Handler: Starting enrollment fetch operation for PeriodoLetivo %d...", params.IdPeriodoLetivo)
		errChan := make(chan error, 1)

		go func() {
			log.Println("Handler Goroutine: Starting client.FetchEnrollmentsFiltered...")
			err := client.FetchEnrollmentsFiltered(ctx, params)
			log.Println("Handler Goroutine: client.FetchEnrollmentsFiltered finished.")
			errChan <- err
		}()

		select {
		case <-ctx.Done():
			log.Printf("Handler: Context cancelled during fetch (timeout/client disconnect): %v", ctx.Err())

			select {
			case fetchErr := <-errChan:
				if fetchErr != nil {
					log.Printf("Handler: Fetch goroutine finished with error: %v", fetchErr)
					return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
						"message": "Fetch operation was cancelled and ended with error",
						"details": fetchErr.Error(),
					})
				}
			default:
			}
			return c.Status(fiber.StatusRequestTimeout).JSON(fiber.Map{
				"message": "Fetch operation timed out or was cancelled by client",
				"details": ctx.Err().Error(),
			})
		case fetchErr := <-errChan:
			if fetchErr != nil {
				log.Printf("Handler: Error during enrollment fetch: %v", fetchErr)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"message": "Failed to fetch enrollments",
					"details": fetchErr.Error(),
				})
			}

			log.Println("Handler: Enrollment fetch completed successfully. Sending OK response.")
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"message": "Enrollments fetched and written to sheet successfully!",
			})
		}
	}
}
