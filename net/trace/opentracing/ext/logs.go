package ext

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

var (
	// The type or "kind" of an error (only for event="error" logs). E.g., "Exception", "OSError"
	// type string
	LogErrorKind = stringLogName("error.kind")

	// For languages that support such a thing (e.g., Java, Python),
	// the actual Throwable/Exception/Error object instance itself.
	// E.g., A java.lang.UnsupportedOperationException instance, a python exceptions.NameError instance
	// type string
	LogErrorObject = stringLogName("error.object")

	// A stable identifier for some notable moment in the lifetime of a Span. For instance, a mutex lock acquisition or release or the sorts of lifetime events in a browser page load described in the Performance.timing specification. E.g., from Zipkin, "cs", "sr", "ss", or "cr". Or, more generally, "initialized" or "timed out". For errors, "error"
	// type string
	LogEvent = stringLogName("event")

	// A concise, human-readable, one-line message explaining the event.
	// E.g., "Could not connect to backend", "Cache invalidation succeeded"
	// type string
	LogMessage = stringLogName("message")

	// A stack trace in platform-conventional format; may or may not pertain to an error. E.g., "File \"example.py\", line 7, in \<module\>\ncaller()\nFile \"example.py\", line 5, in caller\ncallee()\nFile \"example.py\", line 2, in callee\nraise Exception(\"Yikes\")\n"
	// type string
	LogStack = stringLogName("stack")
)

type stringLogName string

// Set adds a string log to the `span`
func (l stringLogName) Set(span opentracing.Span, value string) {
	span.LogFields(log.String(string(l), value))
}
