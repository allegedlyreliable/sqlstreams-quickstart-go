# SQLStreams quickstart (Go)

The finished result of the [SQLStreams quickstart](https://sqlstreams.dev/quickstart/):
a producer and a consumer over one stream in your Postgres database.

## Prerequisites

- Go 1.27+
- Docker Compose for running Postgres

## Run it

Start Postgres:

```sh
docker compose up -d
```

Produce a message:

```sh
go run ./cmd/produce
```

```text
produced message id=1
```

Consume it (blocks until Ctrl-C):

```sh
go run ./cmd/consume
```

```text
received welcome email request for user-123
```

Run the producer again in a second terminal and watch the consumer pick up
each message.
