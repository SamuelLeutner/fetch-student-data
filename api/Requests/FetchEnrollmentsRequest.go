package requests

type FetchEnrollmentsRequest struct {
	OrgId           int    `json:"orgId"`
	IdPeriodoLetivo int    `json:"idPeriodoLetivo"`
	StatusMatricula string `json:"statusMatricula"`
}
