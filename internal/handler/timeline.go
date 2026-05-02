package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type timelineGetter interface {
	GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error)
}

type TimelineHandler struct {
	svc timelineGetter
}

func NewTimelineHandler(svc timelineGetter) *TimelineHandler {
	return &TimelineHandler{svc: svc}
}

type tweetItemResponse struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

func (h *TimelineHandler) GetTimeline(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	items, err := h.svc.GetTimeline(r.Context(), userID, nil, nil)
	if err != nil {
		writeJSON(w, domainErrToStatus(err), map[string]string{"error": err.Error()})
		return
	}
	resp := make([]tweetItemResponse, len(items))
	for i, item := range items {
		resp[i] = tweetItemResponse{
			ID:        item.ID.String(),
			UserID:    item.UserID.String(),
			Username:  item.Username,
			Content:   item.Content,
			CreatedAt: item.CreatedAt,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tweets": resp})
}
