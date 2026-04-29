package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration by path, method and status code",
		Buckets: prometheus.DefBuckets,
	}, []string{"path", "method", "status"})

	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests by path, method and status code",
	}, []string{"path", "method", "status"})

	TimelineCacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "timeline_cache_hits_total",
		Help: "Timelines served from Redis cache",
	})

	TimelineCacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "timeline_cache_misses_total",
		Help: "Timeline cache misses that fell back to Postgres",
	})

	FanoutDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fanout_duration_seconds",
		Help:    "Time spent by tweet consumer fanning out to Redis",
		Buckets: prometheus.DefBuckets,
	})

	RabbitMQMessagesProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rabbitmq_messages_processed_total",
		Help: "Messages successfully processed by a consumer",
	}, []string{"queue"})

	RabbitMQMessagesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rabbitmq_messages_failed_total",
		Help: "Messages that failed during consumer processing",
	}, []string{"queue"})

	RabbitMQQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "rabbitmq_queue_depth",
		Help: "Number of messages pending in a RabbitMQ queue",
	}, []string{"queue"})

	DBQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db_query_duration_seconds",
		Help:    "Postgres query latency by SQL operation (SELECT/INSERT/etc.)",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	DBConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_connections_active",
		Help: "Number of active connections in the Postgres pool",
	})

	DBErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "db_errors_total",
		Help: "Postgres errors by type",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(
		HTTPRequestDuration,
		HTTPRequestsTotal,
		TimelineCacheHitsTotal,
		TimelineCacheMissesTotal,
		FanoutDuration,
		RabbitMQMessagesProcessed,
		RabbitMQMessagesFailed,
		RabbitMQQueueDepth,
		DBQueryDuration,
		DBConnectionsActive,
		DBErrorsTotal,
	)
}
