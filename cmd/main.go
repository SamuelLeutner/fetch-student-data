package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/SamuelLeutner/fetch-student-data/api"
	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/services"
)

func main() {
	config.Init()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	credsPath := config.AppConfig.CredentialsFilePath
	if _, err := os.Stat(credsPath); os.IsNotExist(err) {
		exePath, err := os.Executable()
		if err != nil {
			log.Fatalf("Fatal error: Could not get executable path: %v", err)
		}
		exeDir := filepath.Dir(exePath)
		credsPath = filepath.Join(exeDir, "credentials.json")
		log.Printf("Credentials file not found at '%s', trying '%s'", config.AppConfig.CredentialsFilePath, credsPath)
		config.AppConfig.CredentialsFilePath = credsPath
		if _, err := os.Stat(credsPath); os.IsNotExist(err) {
			log.Fatalf("Fatal error: Credentials file not found at either '%s' or '%s'", config.AppConfig.CredentialsFilePath, credsPath)
		} else if err != nil {
			log.Fatalf("Fatal error checking credentials file at '%s': %v", credsPath, err)
		}
	} else if err != nil {
		log.Fatalf("Fatal error checking credentials file at '%s': %v", credsPath, err)
	}

	sheetsWriter, err := services.NewGoogleSheetsWriter(
		config.AppConfig.SpreadsheetID,
		config.AppConfig.CredentialsFilePath,
		config.AppConfig.MaxRetries,
		config.AppConfig.RetryDelay,
	)
	if err != nil {
		log.Fatalf("Fatal error creating GoogleSheetsWriter: %v", err)
	}

	client := services.NewJacadClient(&config.AppConfig, sheetsWriter)

	app := api.SetupRouter(client, &config.AppConfig)

	listenAddr := ":3000"

	log.Printf("Starting Fiber server on %s...", listenAddr)

	if err := app.Listen(listenAddr); err != nil {
		log.Fatalf("Fatal error starting Fiber server: %v", err)
	}

	log.Println("\nMain process completed (Fiber server stopped).")
}
