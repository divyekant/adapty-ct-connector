package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var authToken string

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	authToken = os.Getenv("AUTH_TOKEN")
	if authToken == "" {
		slog.Error("AUTH_TOKEN env var not set")
		os.Exit(1)
	}
}

func handler(ctx context.Context, req events.APIGatewayCustomAuthorizerRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	provided := strings.TrimPrefix(req.AuthorizationToken, "Bearer ")
	provided = strings.TrimSpace(provided)

	if subtle.ConstantTimeCompare([]byte(provided), []byte(authToken)) != 1 {
		return events.APIGatewayCustomAuthorizerResponse{}, errors.New("Unauthorized")
	}

	return events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: "adapty-webhook",
		PolicyDocument: events.APIGatewayCustomAuthorizerPolicy{
			Version: "2012-10-17",
			Statement: []events.IAMPolicyStatement{{
				Action:   []string{"execute-api:Invoke"},
				Effect:   "Allow",
				Resource: []string{req.MethodArn},
			}},
		},
	}, nil
}

func main() {
	lambda.Start(handler)
}
