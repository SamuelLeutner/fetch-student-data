package models

import (
	"github.com/SamuelLeutner/fetch-student-data/utils"
)

type Period struct {
	OrgID                            int         `json:"idOrg"`
	Organizacao                      string      `json:"organizacao"`
	IDPeriodoLetivo                  int         `json:"idPeriodoLetivo"`
	PeriodoLetivo                    string      `json:"periodoLetivo"`
	IDEdital                         int         `json:"idEdital"`
	Descricao                        string      `json:"descricao"`
	FormulaNota                      string      `json:"formulaNota"`
	StatusEdital                     string      `json:"statusEdital"`
	DataInicio                       *utils.Date `json:"dataInicio"`
	DataTermino                      *utils.Date `json:"dataTermino"`
	DataVencimentoBoleto             *utils.Date `json:"dataVencimentoBoleto"`
	MeioPagamento                    string      `json:"meioPagamento"`
	UtilizarVencimentoDinamicoBoleto int         `json:"utilizarVencimentoDinamicoBoleto"`
	DiasVencimentoDinamicoBoleto     interface{} `json:"diasVencimentoDinamicoBoleto"`
}
