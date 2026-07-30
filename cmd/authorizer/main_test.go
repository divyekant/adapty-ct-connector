package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

type fakeTokenSource struct {
	tokens map[string]string
	calls  int
	err    error
}

func (f *fakeTokenSource) Token(ctx context.Context, tenant string) (string, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	tok, ok := f.tokens[tenant]
	if !ok {
		return "", errTokenNotFound
	}
	return tok, nil
}

const methodArn = "arn:aws:execute-api:us-east-1:009910375489:oocs8tcjpa/prod/POST/ingest/super-test"

func TestParseTenant(t *testing.T) {
	cases := []struct {
		name    string
		arn     string
		want    string
		wantErr bool
	}{
		{"valid", methodArn, "super-test", false},
		{"other tenant", "arn:aws:execute-api:us-east-1:1:api/prod/POST/ingest/acme", "acme", false},
		{"missing tenant segment", "arn:aws:execute-api:us-east-1:1:api/prod/POST/ingest", "", true},
		{"wrong route", "arn:aws:execute-api:us-east-1:1:api/prod/POST/other/x", "", true},
		{"empty tenant", "arn:aws:execute-api:us-east-1:1:api/prod/POST/ingest/", "", true},
		{"path traversal rejected", "arn:aws:execute-api:us-east-1:1:api/prod/POST/ingest/../etc", "", true},
		{"invalid chars rejected", "arn:aws:execute-api:us-east-1:1:api/prod/POST/ingest/a%2Fb", "", true},
		{"garbage", "not-an-arn", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTenant(tc.arn)
			if tc.wantErr != (err != nil) {
				t.Fatalf("parseTenant(%q) err = %v, wantErr %v", tc.arn, err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("parseTenant(%q) = %q, want %q", tc.arn, got, tc.want)
			}
		})
	}
}

func newTestAuthorizer(src tokenSource, ttl time.Duration) *authorizer {
	return &authorizer{tokens: src, cacheTTL: ttl, cache: map[string]cacheEntry{}}
}

func req(token string) events.APIGatewayCustomAuthorizerRequest {
	return events.APIGatewayCustomAuthorizerRequest{
		Type:               "TOKEN",
		AuthorizationToken: token,
		MethodArn:          methodArn,
	}
}

func TestHandlerValidToken(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"super-test": "sekret"}}
	a := newTestAuthorizer(src, 0)

	resp, err := a.handle(context.Background(), req("Bearer sekret"))
	if err != nil {
		t.Fatalf("expected allow, got err %v", err)
	}
	if len(resp.PolicyDocument.Statement) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(resp.PolicyDocument.Statement))
	}
	st := resp.PolicyDocument.Statement[0]
	if st.Effect != "Allow" {
		t.Fatalf("expected Allow, got %s", st.Effect)
	}
	if len(st.Resource) != 1 || st.Resource[0] != methodArn {
		t.Fatalf("policy must be scoped to the request ARN, got %v", st.Resource)
	}
}

func TestHandlerWrongToken(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"super-test": "sekret"}}
	a := newTestAuthorizer(src, 0)

	if _, err := a.handle(context.Background(), req("Bearer nope")); err == nil {
		t.Fatal("expected unauthorized for wrong token")
	}
}

func TestHandlerUnknownTenant(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"other": "sekret"}}
	a := newTestAuthorizer(src, 0)

	if _, err := a.handle(context.Background(), req("Bearer sekret")); err == nil {
		t.Fatal("expected unauthorized for unknown tenant")
	}
}

func TestHandlerMissingBearerPrefix(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"super-test": "sekret"}}
	a := newTestAuthorizer(src, 0)

	if _, err := a.handle(context.Background(), req("sekret")); err == nil {
		t.Fatal("expected unauthorized without Bearer prefix")
	}
}

func TestHandlerTokenSourceFailureIsUnauthorized(t *testing.T) {
	src := &fakeTokenSource{err: errors.New("ssm down")}
	a := newTestAuthorizer(src, 0)

	if _, err := a.handle(context.Background(), req("Bearer sekret")); err == nil {
		t.Fatal("expected unauthorized when token source fails (fail closed)")
	}
}

func TestHandlerCachesTokenWithinTTL(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"super-test": "sekret"}}
	a := newTestAuthorizer(src, time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := a.handle(context.Background(), req("Bearer sekret")); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("expected 1 backend call with warm cache, got %d", src.calls)
	}
}

func TestHandlerNegativeCachesUnknownTenant(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{}}
	a := newTestAuthorizer(src, time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := a.handle(context.Background(), req("Bearer sekret")); err == nil {
			t.Fatal("expected unauthorized")
		}
	}
	if src.calls != 1 {
		t.Fatalf("expected 1 backend call with negative cache, got %d", src.calls)
	}
}

func TestHandlerZeroTTLDisablesCache(t *testing.T) {
	src := &fakeTokenSource{tokens: map[string]string{"super-test": "sekret"}}
	a := newTestAuthorizer(src, 0)

	for i := 0; i < 3; i++ {
		if _, err := a.handle(context.Background(), req("Bearer sekret")); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if src.calls != 3 {
		t.Fatalf("expected 3 backend calls with TTL 0, got %d", src.calls)
	}
}
