package handler

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"uala/internal/metrics"
)

func NewRouter(
	user *UserHandler,
	tweet *TweetHandler,
	follow *FollowHandler,
	timeline *TimelineHandler,
) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /users", user.Create)
	mux.HandleFunc("POST /tweets", tweet.Create)
	mux.HandleFunc("POST /follow", follow.Follow)
	mux.HandleFunc("GET /timeline", timeline.GetTimeline)
	mux.Handle("GET /metrics", promhttp.Handler())
	return metrics.Middleware(mux)
}
