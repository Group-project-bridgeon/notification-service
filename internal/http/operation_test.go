package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	apphttp "notification-service/internal/http"
)

type mockOpRepo struct {
	operations map[uuid.UUID]*domain.Operation
}

func newMockOpRepo() *mockOpRepo {
	return &mockOpRepo{
		operations: make(map[uuid.UUID]*domain.Operation),
	}
}

func (m *mockOpRepo) Upsert(_ context.Context, op *domain.Operation) error {
	existing, ok := m.operations[op.ID]
	if ok {
		if op.LastEventOccurredAt.Before(existing.LastEventOccurredAt) {
			return nil // ignore older out-of-order event
		}
		if op.StartedAt.After(existing.StartedAt) {
			op.StartedAt = existing.StartedAt
		}
	}
	m.operations[op.ID] = op
	return nil
}

func (m *mockOpRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Operation, error) {
	op, ok := m.operations[id]
	if !ok {
		return nil, domain.NewNotFoundError("operation not found")
	}
	return op, nil
}

func (m *mockOpRepo) List(_ context.Context, filter domain.OperationFilter) ([]*domain.Operation, int, error) {
	var list []*domain.Operation
	for _, o := range m.operations {
		if filter.ActorUserID != nil {
			if o.ActorUserID == nil || *o.ActorUserID != *filter.ActorUserID {
				continue
			}
		}
		if filter.EntityType != "" && o.Entity.Type != filter.EntityType {
			continue
		}
		if filter.EntityID != nil && o.Entity.ID != *filter.EntityID {
			continue
		}
		list = append(list, o)
	}
	return list, len(list), nil
}

func TestOperationHandler_GetByID(t *testing.T) {
	repo := newMockOpRepo()
	cfg := &config.Config{
		Env:      "test",
		AuthMode: "dev",
	}
	router := apphttp.NewRouter(apphttp.RouterConfig{
		Config:        cfg,
		OperationRepo: repo,
	})

	actorID := uuid.New()
	otherUserID := uuid.New()
	adminID := uuid.New()
	opID := uuid.New()
	entityID := uuid.New()
	now := time.Now().UTC()
	comp := "completed"

	testOp := &domain.Operation{
		ID:          opID,
		Type:        "sales_order.confirm",
		Entity:      domain.OperationEntity{Type: "sales_order", ID: entityID},
		ActorUserID: &actorID,
		State:       domain.OperationStateFailed,
		Error: &domain.OperationError{
			Code:    "insufficient_stock",
			Message: "Only 6 available",
			Details: []domain.ErrorDetail{},
		},
		Compensation:        &comp,
		CorrelationID:       uuid.New(),
		StartedAt:           now.Add(-1 * time.Minute),
		FinishedAt:          &now,
		LastEventOccurredAt: now,
	}
	_ = repo.Upsert(context.Background(), testOp)

	// 1. Actor can view their own operation -> 200 OK
	{
		req := httptest.NewRequest("GET", "/operations/"+opID.String(), nil)
		req.Header.Set("X-Dev-User-Id", actorID.String())
		req.Header.Set("X-Dev-Roles", "sales")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for actor, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp struct {
			Data domain.OperationResponse `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed decoding response: %v", err)
		}
		if resp.Data.ID != opID || resp.Data.State != domain.OperationStateFailed {
			t.Fatalf("unexpected data: %+v", resp.Data)
		}
		if resp.Data.Error == nil || resp.Data.Error.Code != "insufficient_stock" {
			t.Fatalf("expected error insufficient_stock, got %+v", resp.Data.Error)
		}
		if resp.Data.Compensation == nil || *resp.Data.Compensation != "completed" {
			t.Fatalf("expected compensation completed, got %v", resp.Data.Compensation)
		}
	}

	// 2. Admin or manager can view any operation -> 200 OK
	{
		req := httptest.NewRequest("GET", "/operations/"+opID.String(), nil)
		req.Header.Set("X-Dev-User-Id", adminID.String())
		req.Header.Set("X-Dev-Roles", "admin")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for admin, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// 3. Other unrelated user gets 404 not_found
	{
		req := httptest.NewRequest("GET", "/operations/"+opID.String(), nil)
		req.Header.Set("X-Dev-User-Id", otherUserID.String())
		req.Header.Set("X-Dev-Roles", "warehouse")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for unrelated user, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// 4. Nonexistent operation returns 404 not_found
	{
		req := httptest.NewRequest("GET", "/operations/"+uuid.New().String(), nil)
		req.Header.Set("X-Dev-User-Id", actorID.String())
		req.Header.Set("X-Dev-Roles", "sales")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for nonexistent ID, got %d", rec.Code)
		}
	}
}

func TestOperationHandler_List(t *testing.T) {
	repo := newMockOpRepo()
	cfg := &config.Config{
		Env:      "test",
		AuthMode: "dev",
	}
	router := apphttp.NewRouter(apphttp.RouterConfig{
		Config:        cfg,
		OperationRepo: repo,
	})

	user1 := uuid.New()
	user2 := uuid.New()
	orderID := uuid.New()

	_ = repo.Upsert(context.Background(), &domain.Operation{
		ID:                  uuid.New(),
		Type:                "sales_order.confirm",
		Entity:              domain.OperationEntity{Type: "sales_order", ID: orderID},
		ActorUserID:         &user1,
		State:               domain.OperationStatePending,
		CorrelationID:       uuid.New(),
		StartedAt:           time.Now().UTC(),
		LastEventOccurredAt: time.Now().UTC(),
	})

	_ = repo.Upsert(context.Background(), &domain.Operation{
		ID:                  uuid.New(),
		Type:                "delivery.dispatch",
		Entity:              domain.OperationEntity{Type: "delivery", ID: uuid.New()},
		ActorUserID:         &user2,
		State:               domain.OperationStateSucceeded,
		CorrelationID:       uuid.New(),
		StartedAt:           time.Now().UTC(),
		LastEventOccurredAt: time.Now().UTC(),
	})

	// 1. Regular user1 sees only their operations
	{
		req := httptest.NewRequest("GET", "/operations", nil)
		req.Header.Set("X-Dev-User-Id", user1.String())
		req.Header.Set("X-Dev-Roles", "sales")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp struct {
			Data []domain.OperationResponse `json:"data"`
			Meta apphttp.ListMeta           `json:"meta"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		if len(resp.Data) != 1 || resp.Data[0].Type != "sales_order.confirm" {
			t.Fatalf("expected 1 operation for user1, got %d", len(resp.Data))
		}
	}

	// 2. Manager sees all operations and can filter by entity_type
	{
		req := httptest.NewRequest("GET", "/operations?entity_type=delivery", nil)
		req.Header.Set("X-Dev-User-Id", uuid.New().String())
		req.Header.Set("X-Dev-Roles", "manager")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp struct {
			Data []domain.OperationResponse `json:"data"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		if len(resp.Data) != 1 || resp.Data[0].Type != "delivery.dispatch" {
			t.Fatalf("expected 1 delivery operation, got %d", len(resp.Data))
		}
	}
}
