// cmd/produce/main.go
package main

import (
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
	if _, err := emails.Register(ctx, nil); err != nil {
		return err
	}

	producer, err := emails.Producer().Register(ctx, nil)
	if err != nil {
		return err
	}

	produced, err := producer.Produce(ctx, &WelcomeEmail{UserId: "user-123"}, nil)
	if err != nil {
		return err
	}
	fmt.Printf("produced message id=%d\n", produced.Id)
	return nil
}
