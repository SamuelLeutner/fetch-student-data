package handlers

import (
	"context"
	"log"
	"time"

	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/services"
	"github.com/gofiber/fiber/v3"
)

var params struct {
	IdPeriodoLetivo string `json:"idPeriodoLetivo"`
	StatusMatricula string `json:"statusMatricula"`
}

func CreateFetchEnrollmentsHandler(client *services.JacadClient, appConfig *config.Config) fiber.Handler {
	return func(c fiber.Ctx) error {
		if params.IdPeriodoLetivo == "" {
			log.Println("idPeriodoLetivo is missing")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status":  "error",
				"message": "idPeriodoLetivo is required",
			})
		}

		ctx, cancel := context.WithTimeout(c.Context(), 10*time.Minute)
		defer cancel()

		err := client.FetchEnrollmentsFiltered(ctx, params.IdPeriodoLetivo, params.StatusMatricula)
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
			"message": "Enrollments fetch and write initiated successfully (check logs for details)",
		})
	}
}
