package http

type BaseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
