package handler

import (
	"context"
	"net/http"

	"uala/internal/domain"
)

type userCreator interface {
	CreateUser(ctx context.Context, username string) (*domain.User, error)
}

type UserHandler struct {
	svc userCreator
}

func NewUserHandler(svc userCreator) *UserHandler {
	return &UserHandler{svc: svc}
}

func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
	}
	if err := parseJSON(r, &req); err != nil || req.Username == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username is required"})
		return
	}
	user, err := h.svc.CreateUser(r.Context(), req.Username)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": user.ID.String()})
}
