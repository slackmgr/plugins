package dynamodb

import (
	"errors"
	"time"
)

// Option is a functional option for configuring a [Client].
type Option func(*Options)

// Options holds the configuration for a [Client]. Use [Option] functions
// (such as [WithAlertsTimeToLive] or [WithIssuesTimeToLive]) to customise
// the defaults.
type Options struct {
	alertsTimeToLive time.Duration
	issuesTimeToLive time.Duration
	dynamoDBAPI      API
	clock            func() time.Time
}

func newOptions() *Options {
	return &Options{
		alertsTimeToLive: 30 * 24 * time.Hour,
		issuesTimeToLive: 180 * 24 * time.Hour,
		clock:            time.Now,
	}
}

func (o *Options) validate() error {
	if o.alertsTimeToLive <= 0 {
		return errors.New("alerts time to live must be greater than zero")
	}

	if o.issuesTimeToLive <= 0 {
		return errors.New("issues time to live must be greater than zero")
	}

	return nil
}

// WithAlertsTimeToLive sets the TTL applied to alert records. The default is
// 30 days. The duration must be greater than zero.
func WithAlertsTimeToLive(d time.Duration) Option {
	return func(o *Options) {
		o.alertsTimeToLive = d
	}
}

// WithIssuesTimeToLive sets the TTL applied to closed issue records. The
// default is 180 days. The duration must be greater than zero.
func WithIssuesTimeToLive(d time.Duration) Option {
	return func(o *Options) {
		o.issuesTimeToLive = d
	}
}

// WithAPI sets a custom [API] implementation. This is useful when a custom
// DynamoDB configuration is required, or for injecting mocks in tests.
func WithAPI(api API) Option {
	return func(o *Options) {
		o.dynamoDBAPI = api
	}
}

// WithClock sets a custom clock function used when computing TTL values.
// Defaults to [time.Now]. This is useful for controlling time in tests.
func WithClock(clock func() time.Time) Option {
	return func(o *Options) {
		o.clock = clock
	}
}
