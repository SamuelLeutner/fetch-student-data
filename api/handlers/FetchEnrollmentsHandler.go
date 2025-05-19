package handlers

import (
	"context"
	"fmt"
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

		if err := c.Bind().Body(params); err != nil {
			log.Printf("Error parsing request body: %v", err)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status":  "error",
				"message": "Invalid request body",
				"details": err.Error(),
			})
		}

		if params.IdPeriodoLetivo == 0 {
			log.Println("idPeriodoLetivo is missing from request body")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status":  "error",
				"message": "idPeriodoLetivo is required in the request body",
			})
		}

		ctx, cancel := context.WithTimeout(c.Context(), 10*time.Minute)
		defer cancel()

		err := client.FetchEnrollmentsFiltered(ctx, params)
		if err != nil {
			log.Printf("Error during filtered enrollment fetch: %v", err)

			if ctx.Err() != nil {
				return c.Status(fiber.StatusRequestTimeout).JSON(fiber.Map{
					"status":  "error",
					"message": "Fetch operation timed out or was cancelled",
					"details": err.Error(),
				})
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"status":  "error",
				"message": "Failed to fetch or write enrollments",
				"details": err.Error(),
			})
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status":  "success",
			"message": fmt.Sprintf("Enrollments fetch and write initiated successfully for %d with status %s (check logs for details)", params.IdPeriodoLetivo, params.StatusMatricula),
		})
	}
}
