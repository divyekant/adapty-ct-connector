.PHONY: build build-connector build-lambda build-authorizer build-backfill test clean

build: build-connector build-lambda build-authorizer build-backfill

build-connector:
	CGO_ENABLED=0 go build -o bin/connector ./cmd/connector

build-lambda:
	mkdir -p bin/lambda
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/lambda/bootstrap ./cmd/lambda
	cd bin/lambda && zip -j ../lambda.zip bootstrap

build-authorizer:
	mkdir -p bin/authorizer
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o bin/authorizer/bootstrap ./cmd/authorizer
	cd bin/authorizer && zip -j ../authorizer.zip bootstrap

build-backfill:
	CGO_ENABLED=0 go build -o bin/backfill ./cmd/backfill

test:
	go test ./... -v

clean:
	rm -rf bin/
