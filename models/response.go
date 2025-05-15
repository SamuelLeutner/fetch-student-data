package models

type APIResponse[T any] struct {
	Page     *Page `json:"page"` 
	Elements []T   `json:"elements"`
}

type AuthResponse struct {
	Token string `json:"token"`
}