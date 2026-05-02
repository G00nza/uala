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

	afterStr := r.URL.Query().Get("after")
	beforeStr := r.URL.Query().Get("before")

	if afterStr != "" && beforeStr != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after and before are mutually exclusive"})
		return
	}

	var after, before *uuid.UUID
	if afterStr != "" {
		id, err := uuid.Parse(afterStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "after must be a valid UUID"})
			return
		}
		after = &id
	}
	if beforeStr != "" {
		id, err := uuid.Parse(beforeStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "before must be a valid UUID"})
			return
		}
		before = &id
	}

	items, err := h.svc.GetTimeline(r.Context(), userID, after, before)
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

	var nextCursor, prevCursor *string
	if len(items) > 0 {
		first := items[0].ID.String()
		last := items[len(items)-1].ID.String()
		prevCursor = &first
		nextCursor = &last
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tweets":      resp,
		"next_cursor": nextCursor,
		"prev_cursor": prevCursor,
	})
}
