package handler

import (
	"context"
	"net/http"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type tweetCreator interface {
	Execute(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error)
}

type TweetHandler struct {
	svc tweetCreator
}

func NewTweetHandler(svc tweetCreator) *TweetHandler {
	return &TweetHandler{svc: svc}
}

func (h *TweetHandler) Create(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	var req struct {
		Content string `json:"content"`
	}
	if err := parseJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	tweet, err := h.svc.Execute(r.Context(), userID, req.Content)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": tweet.ID.String()})
}
