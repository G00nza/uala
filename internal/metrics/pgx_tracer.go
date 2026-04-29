package metrics

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type traceStartKey struct{}
type traceSQLKey struct{}

// PgxTracer implements pgx.QueryTracer to record db_query_duration_seconds and db_errors_total.
type PgxTracer struct{}

func (t *PgxTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = context.WithValue(ctx, traceStartKey{}, time.Now())
	ctx = context.WithValue(ctx, traceSQLKey{}, data.SQL)
	return ctx
}

func (t *PgxTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(traceStartKey{}).(time.Time)
	if !ok {
		return
	}
	sql, _ := ctx.Value(traceSQLKey{}).(string)
	op := sqlOperation(sql)
	DBQueryDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if data.Err != nil {
		DBErrorsTotal.WithLabelValues("query").Inc()
	}
}

func sqlOperation(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "unknown"
	}
	return strings.ToUpper(fields[0])
}
