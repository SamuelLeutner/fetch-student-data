package handlers

import (
	"github.com/gofiber/fiber/v3"
)

func HandlePing(c fiber.Ctx) error { 
	response := fiber.Map{ 
		"status":  "ok",
		"message": "pong",
	}
	
	return c.JSON(response) 
}