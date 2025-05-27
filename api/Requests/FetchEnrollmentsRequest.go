package requests

type FetchEnrollmentsRequest struct {
	OrgId           int    `query:"orgId"`
	IdPeriodoLetivo int    `query:"idPeriodoLetivo"`
	StatusMatricula string `query:"statusMatricula"`
}
