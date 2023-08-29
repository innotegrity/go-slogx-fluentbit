package slogxfluentbit_test

// TODO: implement testing and benchmarks

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.innotegrity.dev/errorx"
	"go.innotegrity.dev/slogx"
	slogxfluentbit "go.innotegrity.dev/slogx-fluentbit"
	"golang.org/x/exp/slog"
)

func TestFluentBit1(t *testing.T) {
	handler, err := slogxfluentbit.NewFluentBitHandler(slogxfluentbit.FluentBitHandlerOptions{
		Level: slogx.LevelTrace,
		URL:   "http://localhost:8888",
	})
	if err != nil {
		t.Errorf("failed to create Fluent Bit Handler: %s", err.Error())
		return
	}
	logger := slogx.Wrap(slog.New(handler))
	defer logger.Shutdown(true)

	logger.Trace("this is a trace message")
	logger.Debug("this is a debug message")
	logger.Info("this is an info message")
	logger.Notice("this is a notice message")
	logger.Warn("this is a warning message")
	logger = slogx.Wrap(logger.With(slog.String("root_key", "1")).WithGroup("group1").With(slog.String("k1", "v1")).WithGroup("nested").With(slog.String("logger_name", "frodo")))
	logger.Error("this is an error message")
	logger.Fatal("this is a fatal message")
	logger.Panic("this is a panic message")

	logger.Info("this is an info message with attributes",
		slog.Float64("pie", 3.14),
		slog.String("attr", "Value1"),
		slog.Int("attr2", 100),
		slog.Duration("took", time.Second*5),
		slog.Time("now", time.Now()),
		slog.Group("group",
			slog.String("group1Attr", "value")))
	logger.Error("this is an error message with attributes",
		slog.Float64("pie", 3.141579),
		slog.String("attr", "Value1"),
		slogx.Err("error", errors.New("this is the error message")),
		slogx.ErrX("extended_error", &ErrTest{
			Value1: "important",
			Value2: 1234,
			Err:    errors.New("some error"),
			NestedErr: []errorx.Error{
				&ErrTest{
					Value1: "not so important",
					Value2: 3345,
					Err:    errors.New("some other error"),
				},
			},
		}),
		slog.Any("admin", User{
			Username: "admin",
			Password: "admin123",
			Addresses: []Address{
				{
					Street:     "1234 Acme Way",
					City:       "New York",
					PostalCode: "12345",
					Country:    "United States",
				},
				{
					Street:     "555 Sunset Blvd",
					City:       "Hollywood",
					PostalCode: "90028",
					Country:    "United States",
				},
			},
		}),
		slog.Int("attr2", 100),
		slog.Group("group",
			slog.String("group1Attr", "value"),
			slog.String("better", "value"),
			slog.Int("a", 1),
		),
		slog.Group("user",
			slog.String("name", "josh"),
			slog.String("email", "josh@josh.com"),
		),
	)

}

type User struct {
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	Addresses []Address `json:"addresses"`
}

type Address struct {
	Street     string `json:"street"`
	City       string `json:"city"`
	PostalCode string `json:"postal_code"`
	Country    string `json:"country"`
}

func (u User) LogValue() slog.Value {
	addrAttr := []any{}
	for i, addr := range u.Addresses {
		addrAttr = append(addrAttr, slog.Group(
			fmt.Sprintf("%03d", i),
			slog.String("street", addr.Street),
			slog.String("city", addr.City),
			slog.String("postal_code", addr.PostalCode),
			slog.String("country", addr.Country),
		))
	}
	return slog.GroupValue(
		slog.String("username", u.Username),
		slog.String("password", "********"),
		slog.Group("addresses", addrAttr...),
	)
}

const (
	ErrTestCode = 1751
)

type ErrTest struct {
	Err       error
	Value1    string
	Value2    int
	NestedErr []errorx.Error
}

// InternalError returns the internal standard error object if there is one or nil if none is set.
func (e *ErrTest) InternalError() error {
	return e.Err
}

// Error returns the string version of the error.
func (e *ErrTest) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("an error has occurred: %s", e.Err.Error())
	}
	return "an error has occurred"
}

// Code returns the corresponding error code.
func (e *ErrTest) Code() int {
	return ErrTestCode
}

func (e *ErrTest) Attrs() map[string]any {
	return map[string]any{
		"value1": e.Value1,
		"value2": e.Value2,
	}
}

func (e *ErrTest) NestedErrors() []errorx.Error {
	return e.NestedErr
}
