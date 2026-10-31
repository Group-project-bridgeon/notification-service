package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"notification-service/internal/domain"
	"notification-service/internal/repository"
)

// PreferenceChannelsPayload represents the in_app and email channel preference maps.
type PreferenceChannelsPayload struct {
	InApp map[string]bool `json:"in_app"`
	Email map[string]bool `json:"email"`
}

// PreferenceResponse represents the JSON response wrapper for preferences.
type PreferenceResponse struct {
	Channels PreferenceChannelsPayload `json:"channels"`
}

type UpdatePreferencesRequest struct {
	Channels PreferenceChannelsPayload `json:"channels"`
}

// PreferenceHandler handles user notification channel preference endpoints.
type PreferenceHandler struct {
	repo repository.PreferenceRepository
}

// NewPreferenceHandler creates a PreferenceHandler.
func NewPreferenceHandler(repo repository.PreferenceRepository) *PreferenceHandler {
	return &PreferenceHandler{repo: repo}
}

// Get handles GET /me/notification-preferences.
func (h *PreferenceHandler) Get(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	prefs, err := h.repo.GetByUser(c.Request.Context(), userID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed retrieving preferences", nil)
		return
	}

	// Base defaults
	inAppMap := map[string]bool{
		"approval":   true,
		"purchasing": true,
		"inventory":  true,
		"security":   true,
		"saga":       true,
		"sales":      true,
		"delivery":   true,
		"finance":    true,
	}

	emailMap := map[string]bool{
		"approval":   true,
		"purchasing": false,
		"inventory":  false,
		"security":   true,
		"saga":       true,
		"sales":      false,
		"delivery":   false,
		"finance":    false,
	}

	// Overlay stored preferences
	for _, p := range prefs {
		catStr := string(p.Category)
		if p.Channel == domain.ChannelInApp {
			inAppMap[catStr] = p.Enabled
		} else if p.Channel == domain.ChannelEmail {
			emailMap[catStr] = p.Enabled
		}
	}

	RespondSuccess(c, http.StatusOK, PreferenceResponse{
		Channels: PreferenceChannelsPayload{
			InApp: inAppMap,
			Email: emailMap,
		},
	})
}

// Update handles PUT /me/notification-preferences.
func (h *PreferenceHandler) Update(c *gin.Context) {
	userID, ok := GetUserID(c)
	if !ok {
		RespondError(c, http.StatusUnauthorized, domain.CodeUnauthenticated, "Unauthenticated user", nil)
		return
	}

	var req UpdatePreferencesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, domain.CodeValidationFailed, "Invalid request payload JSON", nil)
		return
	}

	// Validate locked items:
	// Rule: security, saga, approval in-app notifications cannot be disabled
	var lockedDetails []domain.ErrorDetail

	if req.Channels.InApp != nil {
		if enabled, exists := req.Channels.InApp["security"]; exists && !enabled {
			lockedDetails = append(lockedDetails, domain.ErrorDetail{
				Field: "channels.in_app.security",
				Issue: "preference_locked",
			})
		}
		if enabled, exists := req.Channels.InApp["saga"]; exists && !enabled {
			lockedDetails = append(lockedDetails, domain.ErrorDetail{
				Field: "channels.in_app.saga",
				Issue: "preference_locked",
			})
		}
		if enabled, exists := req.Channels.InApp["approval"]; exists && !enabled {
			lockedDetails = append(lockedDetails, domain.ErrorDetail{
				Field: "channels.in_app.approval",
				Issue: "preference_locked",
			})
		}
	}

	if len(lockedDetails) > 0 {
		RespondError(c, http.StatusUnprocessableEntity, domain.CodeValidationFailed, "Cannot disable mandatory notification channel", lockedDetails)
		return
	}

	ctx := c.Request.Context()
	now := time.Now().UTC()

	// Persist in_app preferences
	for cat, enabled := range req.Channels.InApp {
		pref := &domain.NotificationPreference{
			UserID:    userID,
			Category:  domain.NotificationCategory(cat),
			Channel:   domain.ChannelInApp,
			Enabled:   enabled,
			UpdatedAt: now,
		}
		if err := h.repo.Upsert(ctx, pref); err != nil {
			RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed updating in_app preference", nil)
			return
		}
	}

	// Persist email preferences
	for cat, enabled := range req.Channels.Email {
		pref := &domain.NotificationPreference{
			UserID:    userID,
			Category:  domain.NotificationCategory(cat),
			Channel:   domain.ChannelEmail,
			Enabled:   enabled,
			UpdatedAt: now,
		}
		if err := h.repo.Upsert(ctx, pref); err != nil {
			RespondError(c, http.StatusInternalServerError, domain.CodeInternalServerError, "Failed updating email preference", nil)
			return
		}
	}

	// Return updated preferences
	RespondSuccess(c, http.StatusOK, PreferenceResponse{
		Channels: req.Channels,
	})
}
