package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

type APIError struct {
	timestamp time.Time
	err       error
	errorCode int
}

func NewAPIError(errorCode int, err error) *APIError {
	return &APIError{
		err:       err,
		errorCode: errorCode,
		timestamp: time.Now(),
	}
}

func (ae APIError) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Status      string    `json:"status"`
		Timestamp   time.Time `json:"timestamp"`
		UserMessage string    `json:"userMessage"`
		ErrorCode   int       `json:"errorCode"`
	}{
		Status:      http.StatusText(ae.errorCode),
		Timestamp:   ae.timestamp,
		UserMessage: ae.err.Error(),
		ErrorCode:   ae.errorCode,
	})
}

func (ae *APIError) ReturnWithError(c echo.Context) error {
	return c.JSON(ae.errorCode, ae)
}
