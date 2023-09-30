package slogxfluentbit

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-resty/resty/v2"
	"go.innotegrity.dev/async"
	"go.innotegrity.dev/generic"
	"go.innotegrity.dev/slogx"
	"go.innotegrity.dev/slogx/formatter"
	"golang.org/x/exp/slog"
)

// fluentBitHandlerOptionsContext can be used to retrieve the options used by the handler from the context.
type fluentBitHandlerOptionsContext struct{}

// FluentBitHandlerOptions holds the options for the Fluent Bit handler.
type FluentBitHandlerOptions struct {
	// ContentType is the mime type to pass to the HTTP listener.
	//
	// By default, this is set to application/json as it is assumed the message being sent will be in JSON format.
	ContentType string

	// EnableAsync will execute the Handle() function in a separate goroutine.
	//
	// When async is enabled, you should be sure to call the Shutdown() function or use the slogx.Shutdown()
	// function to ensure all goroutines are finished and any pending records have been written.
	EnableAsync bool

	// HTTPClient allows for the use of a custom HTTP client for posting the message to the HTTP listener.
	//
	// If nil, a default resty client is used.
	HTTPClient *resty.Client

	// Level is the minimum log level to write to the handler.
	//
	// By default, the level will be set to slog.LevelInfo if not supplied.
	Level slog.Leveler

	// RecordFormatter specifies the formatter to use to format the record before sending it to the HTTP listener.
	//
	// If no formatter is supplied, formatter.DefaultJSONFormatter is used to format the output.
	RecordFormatter formatter.BufferFormatter

	// URL is the URL of the Fluent Bit HTTP listener to post the message to.
	//
	// This is a required option.
	URL string
}

// DefaultFluentBitHandlerOptions returns a default set of options for the handler.
func DefaultFluentBitHandlerOptions() FluentBitHandlerOptions {
	return FluentBitHandlerOptions{
		ContentType:     "application/json",
		HTTPClient:      resty.New(),
		Level:           slog.LevelInfo,
		RecordFormatter: formatter.DefaultJSONFormatter(),
	}
}

// GetFluentBitHandlerOptionsFromContext retrieves the options from the context.
//
// If the options are not set in the context, a set of default options is returned instead.
func GetFluentBitHandlerOptionsFromContext(ctx context.Context) *FluentBitHandlerOptions {
	o := ctx.Value(fluentBitHandlerOptionsContext{})
	if o != nil {
		if opts, ok := o.(*FluentBitHandlerOptions); ok {
			return opts
		}
	}
	opts := DefaultFluentBitHandlerOptions()
	return &opts
}

// AddToContext adds the options to the given context and returns the new context.
func (o *FluentBitHandlerOptions) AddToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, fluentBitHandlerOptionsContext{}, o)
}

// fluentBitHandler is a log handler that writes records to a Fluent Bit HTTP listener.
type fluentBitHandler struct {
	activeGroup string
	attrs       []slog.Attr
	futures     []async.Future
	groups      []string
	options     FluentBitHandlerOptions
}

// NewFluentBitHandler creates a new handler object.
func NewFluentBitHandler(opts FluentBitHandlerOptions) (*fluentBitHandler, error) {
	// validate required options
	if opts.URL == "" {
		return nil, errors.New("URL is required and cannot be empty")
	}

	// set default options
	if opts.ContentType == "" {
		opts.ContentType = "application/json"
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = resty.New()
	}
	if opts.Level == nil {
		opts.Level = slog.LevelInfo
	}

	// create the handler
	return &fluentBitHandler{
		attrs:   []slog.Attr{},
		futures: []async.Future{},
		groups:  []string{},
		options: opts,
	}, nil
}

// Enabled determines whether or not the given level is enabled in this handler.
func (h fluentBitHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.options.Level.Level()
}

// Handle actually handles posting the record to the HTTP listener.
//
// Any attributes duplicated between the handler and record, including within groups, are automaticlaly removed.
// If a duplicate is encountered, the last value found will be used for the attribute's value.
func (h *fluentBitHandler) Handle(ctx context.Context, r slog.Record) error {
	handlerCtx := h.options.AddToContext(ctx)
	if !h.options.EnableAsync {
		return h.handle(handlerCtx, r)
	}

	future := async.Exec(func() any {
		return h.handle(handlerCtx, r)
	})
	h.futures = append(h.futures, future)
	return nil
}

// Shutdown is responsible for cleaning up resources used by the handler.
func (h fluentBitHandler) Shutdown(continueOnError bool) error {
	for _, f := range h.futures {
		if f != nil {
			f.Await()
		}
	}
	return nil
}

// WithAttrs creates a new handler from the existing one adding the given attributes to it.
func (h fluentBitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandler := &fluentBitHandler{
		attrs:   h.attrs,
		futures: h.futures,
		groups:  h.groups,
		options: h.options,
	}
	if h.activeGroup == "" {
		newHandler.attrs = append(newHandler.attrs, attrs...)
	} else {
		newHandler.attrs = append(newHandler.attrs, slog.Group(h.activeGroup, generic.AnySlice(attrs)...))
		newHandler.activeGroup = h.activeGroup
	}
	return newHandler
}

// WithGroup creates a new handler from the existing one adding the given group to it.
func (h fluentBitHandler) WithGroup(name string) slog.Handler {
	newHandler := &fluentBitHandler{
		attrs:   h.attrs,
		futures: h.futures,
		groups:  h.groups,
		options: h.options,
	}
	if name != "" {
		newHandler.groups = append(newHandler.groups, name)
		newHandler.activeGroup = name
	}
	return newHandler
}

// handle is responsible for actually posting the message to the HTTP listener.
func (h fluentBitHandler) handle(ctx context.Context, r slog.Record) error {
	attrs := slogx.ConsolidateAttrs(h.attrs, h.activeGroup, r)

	// format the output into a buffer
	var buf *slogx.Buffer
	var err error
	if h.options.RecordFormatter != nil {
		buf, err = h.options.RecordFormatter.FormatRecord(ctx, r.Time, slogx.Level(r.Level), r.PC, r.Message,
			attrs)
	} else {
		f := formatter.DefaultJSONFormatter()
		buf, err = f.FormatRecord(ctx, r.Time, slogx.Level(r.Level), r.PC, r.Message, attrs)
	}
	if err != nil {
		return err
	}

	// post the message to the HTTP listener
	resp, err := h.options.HTTPClient.R().
		SetHeader("Content-Type", h.options.ContentType).
		SetBody(buf.String()).
		Post(h.options.URL)
	if err != nil {
		return err
	}
	if resp.StatusCode() >= 400 {
		return fmt.Errorf("failed to write message - HTTP status code %d", resp.StatusCode())
	}
	return nil
}
