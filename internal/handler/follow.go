package handler

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type userFollower interface {
	Execute(ctx context.Context, followerID, followeeID uuid.UUID) error
}

type FollowHandler struct {
	svc userFollower
}

func NewFollowHandler(svc userFollower) *FollowHandler {
	return &FollowHandler{svc: svc}
}

func (h *FollowHandler) Follow(w http.ResponseWriter, r *http.Request) {
	followerID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var req struct {
		FolloweeID string `json:"followee_id"`
	}
	if err := parseJSON(r, &req); err != nil || req.FolloweeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "followee_id is required"})
		return
	}
	followeeID, err := uuid.Parse(req.FolloweeID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "followee_id must be a valid UUID"})
		return
	}
	if err := h.svc.Execute(r.Context(), followerID, followeeID); err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, struct{}{})
}
