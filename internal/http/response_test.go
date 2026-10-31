package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
)

func TestRespondSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondSuccess(c, http.StatusOK, map[string]string{"foo": "bar"})

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var res SuccessEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	dataMap, ok := res.Data.(map[string]any)
	if !ok || dataMap["foo"] != "bar" {
		t.Errorf("expected data.foo to be 'bar', got %v", res.Data)
	}
}

func TestRespondList_Pagination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	items := []string{"item1", "item2"}
	RespondList(c, http.StatusOK, items, 2, 10, 45)

	var res ListEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode list response: %v", err)
	}

	if res.Meta.Page != 2 || res.Meta.PageSize != 10 || res.Meta.TotalCount != 45 || res.Meta.TotalPages != 5 {
		t.Errorf("unexpected meta values: %+v", res.Meta)
	}
}

func TestRespondAppError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set(CtxKeyRequestID, "test-req-id")

	appErr := domain.NewValidationError("invalid form input", domain.ErrorDetail{
		Field: "email",
		Issue: "format_invalid",
	})

	RespondAppError(c, appErr)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var res ErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if res.Error.Code != domain.CodeValidationFailed {
		t.Errorf("expected code validation_failed, got %s", res.Error.Code)
	}
	if res.Error.RequestID != "test-req-id" {
		t.Errorf("expected request_id test-req-id, got %s", res.Error.RequestID)
	}
	if len(res.Error.Details) != 1 || res.Error.Details[0].Field != "email" {
		t.Errorf("unexpected error details: %+v", res.Error.Details)
	}
}
