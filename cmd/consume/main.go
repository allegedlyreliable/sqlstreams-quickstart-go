// cmd/consume/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/allegedlyreliable/sqlstreams/client"
)

type WelcomeEmail struct {
	UserId string `json:"user_id"`
}

func (WelcomeEmail) SchemaVersion() int { return 1 } // increment on breaking changes

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := sqlstreams.LifecycleContext(nil)
	defer stop()

	pool, err := sqlstreams.NewPostgresPool(ctx, "username", "password", "localhost", "quickstart", nil)
	if err != nil {
		return err
	}
	defer pool.Close()

	client, err := sqlstreams.NewClient(ctx, pool, nil)
	if err != nil {
		return err
	}

	emails := client.Stream[WelcomeEmail]("signup.welcome-email")
	registered, err := emails.Get(ctx)
	if err != nil {
		return err
	}
	if registered == nil {
		return errors.New("stream not registered -- run the producer first")
	}

	sender := emails.Consumer("email-sender")
	consumer, err := sender.Register(ctx, nil)
	if err != nil {
		return err
	}

	return consumer.Consume(ctx, receiveEmail, nil)
}

func receiveEmail(ctx context.Context, email *WelcomeEmail) error {
	fmt.Printf("received welcome email request for %s\n", email.UserId)
	return nil
}
