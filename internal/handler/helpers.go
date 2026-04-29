package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"uala/internal/domain"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func parseJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func parseUserID(r *http.Request) (uuid.UUID, error) {
	raw := r.Header.Get("X-User-ID")
	if raw == "" {
		return uuid.UUID{}, errors.New("X-User-ID header is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, errors.New("X-User-ID must be a valid UUID")
	}
	return id, nil
}

func domainErrToStatus(err error) int {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, domain.ErrUsernameConflict):
		return http.StatusConflict
	case errors.Is(err, domain.ErrAlreadyFollowing):
		return http.StatusConflict
	case errors.Is(err, domain.ErrSelfFollow),
		errors.Is(err, domain.ErrEmptyUsername),
		errors.Is(err, domain.ErrEmptyContent),
		errors.Is(err, domain.ErrContentTooLong):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
