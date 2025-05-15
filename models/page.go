package models

type Page struct {
	CurrentPage int `json:"currentPage"`
	PageSize    int `json:"pageSize"`
	TotalElements int `json:"totalElements"`
	TotalPages  int `json:"totalPages"`
}