package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
)

func Init() {
	fmt.Println("Initializing configuration...")
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
		return
	}

	log.Println("Loaded .env file successfully")
	AppConfig.UserToken = os.Getenv("USER_TOKEN")
	AppConfig.APIBase = os.Getenv("API_BASE")
	AppConfig.SpreadsheetID = os.Getenv("SPREADSHEET_ID")
	AppConfig.CredentialsFilePath = os.Getenv("CREDENTIALS_FILE_PATH")
}

type Config struct {
	UserToken           string
	APIBase             string
	Endpoints           map[string]string
	Organizations       map[string]Organization
	DefaultOrgSheet     string
	PageSize            int
	MaxPagesPerBatch    int
	MaxParallelRequests int
	RetryDelay          time.Duration
	MaxRetries          int
	AuthTokenExpiry     time.Duration
	SpreadsheetID       string
	CredentialsFilePath string
	EditalStatus        []string
}

type Organization struct {
	ID   int
	Name string
}

var AppConfig = Config{
	UserToken:           "",
	APIBase:             "",
	SpreadsheetID:       "",
	CredentialsFilePath: "",
	Endpoints: map[string]string{
		"AUTH":            "/auth/token",
		"ENROLLMENTS":     "/academico/matriculas",
		"PROCESS_NOTICES": "/processo-seletivo/editais/",
	},
	Organizations: map[string]Organization{
		"EAD":            {ID: 20, Name: "EAD"},
		"POS_EAD":        {ID: 17, Name: "PÓS EAD"},
		"POS_PRESENCIAL": {ID: 9, Name: "PÓS Presencial"},
		"PRESENCIAL":     {ID: 0, Name: "Presencial"},
		"POLICLINICA":    {ID: 4, Name: "Policlínica Uniguairacá"},
		"COLEGIO":        {ID: 15, Name: "Colégio Uniguairacá"},
		"CLINICA":        {ID: 18, Name: "Clínica Integrada"},
	},
	DefaultOrgSheet:     "Outras Matrículas",
	PageSize:            500,
	MaxPagesPerBatch:    50,
	MaxParallelRequests: 10,
	RetryDelay:          2000 * time.Millisecond,
	MaxRetries:          3,
	AuthTokenExpiry:     60 * time.Minute,
	EditalStatus: []string{
		"ABERTO",
		"AGUARDANDO",
	},
}

func GetOrganizationNameByID(orgID int) string {
	for _, org := range AppConfig.Organizations {
		if org.ID == orgID {
			return org.Name
		}
	}
	return ""
}
