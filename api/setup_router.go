package api

import (
	"github.com/SamuelLeutner/fetch-student-data/api/handlers"
	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/services"
	"github.com/gofiber/fiber/v3"
)

func SetupRouter(client *services.JacadClient, appConfig *config.Config) *fiber.App { 

	r := fiber.New()
	api := r.Group("/api/v1")

	api.Get("/ping", handlers.HandlePing)
	api.Get("/fetch-enrollments", handlers.CreateFetchEnrollmentsHandler(client, appConfig)) 

	return r
}