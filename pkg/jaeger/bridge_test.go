package jaeger

import (
	"testing"
	"time"

	v1 "github.com/jaegertracing/jaeger-idl/model/v1"
	jaegerthrift "github.com/jaegertracing/jaeger-idl/thrift-gen/jaeger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
func int64Ptr(i int64) *int64 { return &i }
func float64Ptr(f float64) *float64 { return &f }

func TestConvertTag(t *testing.T) {
	tests := []struct {
		name     string
		input    *jaegerthrift.Tag
		expected v1.KeyValue
	}{
		{
			name:     "string tag",
			input:    &jaegerthrift.Tag{Key: "k", VType: jaegerthrift.TagType_STRING, VStr: strPtr("v")},
			expected: v1.KeyValue{Key: "k", VType: v1.ValueType_STRING, VStr: "v"},
		},
		{
			name:     "bool tag",
			input:    &jaegerthrift.Tag{Key: "k", VType: jaegerthrift.TagType_BOOL, VBool: boolPtr(true)},
			expected: v1.KeyValue{Key: "k", VType: v1.ValueType_BOOL, VBool: true},
		},
		{
			name:     "long tag",
			input:    &jaegerthrift.Tag{Key: "k", VType: jaegerthrift.TagType_LONG, VLong: int64Ptr(42)},
			expected: v1.KeyValue{Key: "k", VType: v1.ValueType_INT64, VInt64: 42},
		},
		{
			name:     "double tag",
			input:    &jaegerthrift.Tag{Key: "k", VType: jaegerthrift.TagType_DOUBLE, VDouble: float64Ptr(3.14)},
			expected: v1.KeyValue{Key: "k", VType: v1.ValueType_FLOAT64, VFloat64: 3.14},
		},
		{
			name:     "binary tag",
			input:    &jaegerthrift.Tag{Key: "k", VType: jaegerthrift.TagType_BINARY, VBinary: []byte{1, 2, 3}},
			expected: v1.KeyValue{Key: "k", VType: v1.ValueType_BINARY, VBinary: []byte{1, 2, 3}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertTag(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestConvertSpan_BasicFields(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow:    0x1234567890ABCDEF,
		TraceIdHigh:   0x1122334455667788,
		SpanId:        0x0102030405060708,
		OperationName: "op",
		Flags:         1,
		StartTime:     1000000, // 1 second in microseconds
		Duration:      500000,  // 0.5 seconds in microseconds
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)

	assert.Equal(t, v1.TraceID{High: 0x1122334455667788, Low: 0x1234567890ABCDEF}, got.TraceID)
	assert.Equal(t, v1.SpanID(0x0102030405060708), got.SpanID)
	assert.Equal(t, "op", got.OperationName)
	assert.Equal(t, v1.Flags(1), got.Flags)
	assert.Equal(t, time.Unix(0, 1000000*int64(time.Microsecond)), got.StartTime)
	assert.Equal(t, 500000*time.Microsecond, got.Duration)
}

func TestConvertSpan_ParentSpanId_CreatesChildOfReference(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow:   0x1,
		TraceIdHigh:  0x0,
		SpanId:       0x2,
		ParentSpanId: 0x1,
		StartTime:    0,
		Duration:     100,
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)
	require.Len(t, got.References, 1)

	ref := got.References[0]
	assert.Equal(t, v1.SpanRefType_CHILD_OF, ref.RefType)
	assert.Equal(t, v1.SpanID(0x1), ref.SpanID)
	assert.Equal(t, v1.TraceID{High: 0x0, Low: 0x1}, ref.TraceID)
}

func TestConvertSpan_NoParentSpanId_NoReferences(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow:   0x1,
		SpanId:       0x2,
		ParentSpanId: 0,
		StartTime:    0,
		Duration:     100,
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)
	assert.Empty(t, got.References)
}

func TestConvertSpan_ExplicitReferencesConvertedCorrectly(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow:  0x1,
		TraceIdHigh: 0x0,
		SpanId:      0x3,
		References: []*jaegerthrift.SpanRef{
			{
				RefType:     jaegerthrift.SpanRefType_CHILD_OF,
				TraceIdLow:  0x1,
				TraceIdHigh: 0x0,
				SpanId:      0x2,
			},
			{
				RefType:     jaegerthrift.SpanRefType_FOLLOWS_FROM,
				TraceIdLow:  0x10,
				TraceIdHigh: 0x0,
				SpanId:      0x20,
			},
		},
		StartTime: 0,
		Duration:  100,
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)
	require.Len(t, got.References, 2)

	assert.Equal(t, v1.SpanRefType_CHILD_OF, got.References[0].RefType)
	assert.Equal(t, v1.SpanID(0x2), got.References[0].SpanID)

	assert.Equal(t, v1.SpanRefType_FOLLOWS_FROM, got.References[1].RefType)
	assert.Equal(t, v1.SpanID(0x20), got.References[1].SpanID)
}

func TestConvertSpan_ParentAlreadyInReferences_NoDuplicate(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow:   0x1,
		TraceIdHigh:  0x0,
		SpanId:       0x3,
		ParentSpanId: 0x2,
		References: []*jaegerthrift.SpanRef{
			{
				RefType:     jaegerthrift.SpanRefType_CHILD_OF,
				TraceIdLow:  0x1,
				TraceIdHigh: 0x0,
				SpanId:      0x2,
			},
		},
		StartTime: 0,
		Duration:  100,
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)
	// ParentSpanId matches the existing ChildOf reference — no duplicate should be added.
	assert.Len(t, got.References, 1)
}

func TestConvertProcess_NilProcess(t *testing.T) {
	got := convertProcess(nil)
	require.NotNil(t, got)
	assert.Equal(t, "", got.ServiceName)
	assert.Empty(t, got.Tags)
}

func TestConvertProcess_WithTagsAndServiceName(t *testing.T) {
	p := &jaegerthrift.Process{
		ServiceName: "my-service",
		Tags: []*jaegerthrift.Tag{
			{Key: "hostname", VType: jaegerthrift.TagType_STRING, VStr: strPtr("host1")},
		},
	}
	got := convertProcess(p)
	require.NotNil(t, got)
	assert.Equal(t, "my-service", got.ServiceName)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "hostname", got.Tags[0].Key)
	assert.Equal(t, "host1", got.Tags[0].VStr)
}

func TestConvertSpan_ProcessSetOnSpan(t *testing.T) {
	proc := &jaegerthrift.Process{ServiceName: "svc"}
	batch := &jaegerthrift.Batch{
		Spans: []*jaegerthrift.Span{
			{TraceIdLow: 1, SpanId: 2, StartTime: 0, Duration: 100},
		},
		Process: proc,
	}

	p := convertProcess(batch.Process)
	span := convertSpan(batch.Spans[0])
	span.Process = p

	require.NotNil(t, span.Process)
	assert.Equal(t, "svc", span.Process.ServiceName)
}

func TestConvertSpan_TagsAndLogs(t *testing.T) {
	thriftSpan := &jaegerthrift.Span{
		TraceIdLow: 0x1,
		SpanId:     0x2,
		StartTime:  0,
		Duration:   100,
		Tags: []*jaegerthrift.Tag{
			{Key: "http.method", VType: jaegerthrift.TagType_STRING, VStr: strPtr("GET")},
		},
		Logs: []*jaegerthrift.Log{
			{
				Timestamp: 1000,
				Fields: []*jaegerthrift.Tag{
					{Key: "event", VType: jaegerthrift.TagType_STRING, VStr: strPtr("error")},
				},
			},
		},
	}

	got := convertSpan(thriftSpan)
	require.NotNil(t, got)

	require.Len(t, got.Tags, 1)
	assert.Equal(t, "http.method", got.Tags[0].Key)
	assert.Equal(t, "GET", got.Tags[0].VStr)

	require.Len(t, got.Logs, 1)
	assert.Equal(t, time.Unix(0, 1000*int64(time.Microsecond)), got.Logs[0].Timestamp)
	require.Len(t, got.Logs[0].Fields, 1)
	assert.Equal(t, "event", got.Logs[0].Fields[0].Key)
}
