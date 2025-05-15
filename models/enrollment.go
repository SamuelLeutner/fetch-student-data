package models

import "github.com/SamuelLeutner/fetch-student-data/utils"

type Enrollment struct {
	IdMatricula   int         `json:"idMatricula"`
	Aluno         *string     `json:"aluno"`
	RA            *string     `json:"ra"`
	Curso         *string     `json:"curso"`
	Turma         *string     `json:"turma"`
	Status        *string     `json:"status"`
	PeriodoLetivo *string     `json:"periodoLetivo"`
	UnidadeFisica *string     `json:"unidadeFisica"`
	Organizacao   *string     `json:"organizacao"`
	OrgID         int         `json:"idOrg"`
	DataMatricula *utils.Date `json:"dataMatricula"`
	DataAtivacao  *utils.Date `json:"dataAtivacao"`
	DataCadastro  *utils.Date `json:"dataCadastro"`
}
