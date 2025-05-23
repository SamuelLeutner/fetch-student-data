package main

import (
	"context"
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

	credsPathForWriterFallback := config.AppConfig.CredentialsJSONBase64
	if os.Getenv("GOOGLE_CREDENTIALS_JSON_BASE64") == "" {
		log.Println("INFO: GOOGLE_CREDENTIALS_JSON_BASE64 not set. GoogleSheetsWriter will try the fallback file path if provided.")
		if credsPathForWriterFallback == "" {
			exePath, err := os.Executable()
			if err != nil {
				log.Printf("FATAL: Could not get executable path: %v", err)
			}
			exeDir := filepath.Dir(exePath)
			credsPathForWriterFallback = filepath.Join(exeDir, "credentials.json")
			log.Printf("INFO: CredentialsJSONBase64 from config is empty. Defaulting fallback path to be next to executable: '%s'", credsPathForWriterFallback)
		}

		if _, err := os.Stat(credsPathForWriterFallback); os.IsNotExist(err) {
			log.Printf("WARN: Fallback credentials file not found at '%s'. GoogleSheetsWriter might attempt Application Default Credentials or fail if no credentials source is available.", credsPathForWriterFallback)
		} else if err != nil {
			log.Printf("ERROR: Error checking fallback credentials file at '%s': %v. GoogleSheetsWriter might still attempt ADC.", credsPathForWriterFallback, err)
		}
	} else {
		log.Println("INFO: GOOGLE_CREDENTIALS_JSON_BASE64 is set. GoogleSheetsWriter will prioritize it.")
	}

	ctx := context.Background()

	sheetsWriter, err := services.NewGoogleSheetsWriter(
		ctx,
		config.AppConfig.SpreadsheetID,
		credsPathForWriterFallback,
		config.AppConfig.MaxRetries,
		config.AppConfig.RetryDelay,
	)
	if err != nil {
		log.Printf("FATAL: Error creating GoogleSheetsWriter: %v", err)
	}

	client := services.NewJacadClient(&config.AppConfig, sheetsWriter)
	app := api.SetupRouter(client, &config.AppConfig)
	listenAddr := os.Getenv("LISTEN_ADDR")

	log.Printf("INFO: Starting Fiber server on %s...", listenAddr)
	if err := app.Listen(listenAddr); err != nil {
		log.Printf("FATAL: Error starting Fiber server: %v", err)
	}

	log.Println("INFO: Main process completed (Fiber server stopped).")
}
