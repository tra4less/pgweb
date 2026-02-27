package jaeger

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	v1 "github.com/jaegertracing/jaeger-idl/model/v1"
	api_v2 "github.com/jaegertracing/jaeger-idl/proto-gen/api_v2"
	jaegerthrift "github.com/jaegertracing/jaeger-idl/thrift-gen/jaeger"
)

// convertTag converts a Thrift Tag to a v1.KeyValue.
func convertTag(t *jaegerthrift.Tag) v1.KeyValue {
	kv := v1.KeyValue{Key: t.Key}
	switch t.VType {
	case jaegerthrift.TagType_BOOL:
		kv.VType = v1.ValueType_BOOL
		if t.VBool != nil {
			kv.VBool = *t.VBool
		}
	case jaegerthrift.TagType_LONG:
		kv.VType = v1.ValueType_INT64
		if t.VLong != nil {
			kv.VInt64 = *t.VLong
		}
	case jaegerthrift.TagType_DOUBLE:
		kv.VType = v1.ValueType_FLOAT64
		if t.VDouble != nil {
			kv.VFloat64 = *t.VDouble
		}
	case jaegerthrift.TagType_BINARY:
		kv.VType = v1.ValueType_BINARY
		kv.VBinary = t.VBinary
	default: // STRING
		kv.VType = v1.ValueType_STRING
		if t.VStr != nil {
			kv.VStr = *t.VStr
		}
	}
	return kv
}

// convertTags converts a slice of Thrift Tags to []v1.KeyValue.
func convertTags(tags []*jaegerthrift.Tag) []v1.KeyValue {
	if len(tags) == 0 {
		return nil
	}
	out := make([]v1.KeyValue, len(tags))
	for i, t := range tags {
		out[i] = convertTag(t)
	}
	return out
}

// convertLogs converts a slice of Thrift Logs to []v1.Log.
func convertLogs(logs []*jaegerthrift.Log) []v1.Log {
	if len(logs) == 0 {
		return nil
	}
	out := make([]v1.Log, len(logs))
	for i, l := range logs {
		out[i] = v1.Log{
			Timestamp: time.Unix(0, l.Timestamp*int64(time.Microsecond)),
			Fields:    convertTags(l.Fields),
		}
	}
	return out
}

// convertSpanRef converts a Thrift SpanRef to a v1.SpanRef.
func convertSpanRef(r *jaegerthrift.SpanRef) v1.SpanRef {
	refType := v1.SpanRefType_CHILD_OF
	if r.RefType == jaegerthrift.SpanRefType_FOLLOWS_FROM {
		refType = v1.SpanRefType_FOLLOWS_FROM
	}
	return v1.SpanRef{
		TraceID: v1.TraceID{
			High: uint64(r.TraceIdHigh),
			Low:  uint64(r.TraceIdLow),
		},
		SpanID:  v1.SpanID(r.SpanId),
		RefType: refType,
	}
}

// convertSpan converts a Thrift Span to a v1.Span, including all fields
// required for Jaeger to index and find the span by trace ID.
func convertSpan(s *jaegerthrift.Span) *v1.Span {
	traceID := v1.TraceID{
		High: uint64(s.TraceIdHigh),
		Low:  uint64(s.TraceIdLow),
	}

	// Build references: start with explicit references, then add ParentSpanId
	// as a ChildOf reference if it is set and not already covered.
	var refs []v1.SpanRef
	for _, r := range s.References {
		refs = append(refs, convertSpanRef(r))
	}
	if s.ParentSpanId != 0 {
		// Only add an implicit parent reference when no explicit ChildOf
		// reference for this parent already exists.
		alreadyPresent := false
		for _, ref := range refs {
			if ref.RefType == v1.SpanRefType_CHILD_OF && ref.SpanID == v1.SpanID(s.ParentSpanId) {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			refs = append(refs, v1.SpanRef{
				TraceID: traceID,
				SpanID:  v1.SpanID(s.ParentSpanId),
				RefType: v1.SpanRefType_CHILD_OF,
			})
		}
	}

	return &v1.Span{
		TraceID:       traceID,
		SpanID:        v1.SpanID(s.SpanId),
		OperationName: s.OperationName,
		References:    refs,
		Flags:         v1.Flags(s.Flags),
		StartTime:     time.Unix(0, s.StartTime*int64(time.Microsecond)),
		Duration:      time.Duration(s.Duration) * time.Microsecond,
		Tags:          convertTags(s.Tags),
		Logs:          convertLogs(s.Logs),
	}
}

// convertProcess converts a Thrift Process to a v1.Process, including its
// tags. A nil input returns an empty (but non-nil) Process so callers can
// always set it on spans without a nil-pointer check.
func convertProcess(p *jaegerthrift.Process) *v1.Process {
	if p == nil {
		return &v1.Process{}
	}
	return &v1.Process{
		ServiceName: p.ServiceName,
		Tags:        convertTags(p.Tags),
	}
}

// HandleTraces returns an http.HandlerFunc that receives Thrift-encoded Jaeger
// spans over HTTP and forwards them to a Jaeger gRPC collector.
func HandleTraces(client api_v2.CollectorServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			log.Println("read body error:", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := r.Body.Close(); err != nil {
			log.Println("body close error:", err)
		}

		if len(data) == 0 {
			log.Println("empty request body")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		transport := thrift.NewTMemoryBuffer()
		if _, err := transport.Write(data); err != nil {
			log.Println("thrift buffer write failed:", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		proto := thrift.NewTBinaryProtocolTransport(transport)
		batch := jaegerthrift.NewBatch()
		if err := batch.Read(context.Background(), proto); err != nil {
			log.Printf("thrift decode failed (len=%d): %v", len(data), err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		spans := make([]*v1.Span, 0, len(batch.Spans))
		proc := convertProcess(batch.Process)
		for _, s := range batch.Spans {
			span := convertSpan(s)
			span.Process = proc
			spans = append(spans, span)
		}

		grpcBatch := v1.Batch{
			Spans:   spans,
			Process: proc,
		}

		req := &api_v2.PostSpansRequest{
			Batch: grpcBatch,
		}

		_, err = client.PostSpans(context.Background(), req)
		if err != nil {
			log.Println("gRPC PostSpans error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		log.Println("forwarded spans:", len(spans))
	}
}
