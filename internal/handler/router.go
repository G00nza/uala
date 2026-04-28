package handler

import "net/http"

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
	return mux
}
