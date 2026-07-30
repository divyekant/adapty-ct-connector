package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

var (
	errUnauthorized  = errors.New("Unauthorized")
	errTokenNotFound = errors.New("token not found for tenant")

	// Tenant slugs are path segments that become SSM parameter path segments;
	// restrict the charset so a crafted URL can't address a different parameter.
	tenantRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// parseTenant extracts the tenant slug from a method ARN of the form
// arn:aws:execute-api:{region}:{account}:{apiId}/{stage}/{verb}/ingest/{tenant}
func parseTenant(methodArn string) (string, error) {
	parts := strings.SplitN(methodArn, "/", 5)
	if len(parts) != 5 || parts[2] != "POST" || parts[3] != "ingest" {
		return "", fmt.Errorf("unexpected method ARN shape: %q", methodArn)
	}
	tenant := parts[4]
	if !tenantRE.MatchString(tenant) {
		return "", fmt.Errorf("invalid tenant segment in ARN: %q", methodArn)
	}
	return tenant, nil
}

// tokenSource resolves the expected auth token for a tenant.
type tokenSource interface {
	Token(ctx context.Context, tenant string) (string, error)
}

// ssmTokenSource reads {prefix}/{tenant}/auth-token from SSM Parameter Store.
type ssmTokenSource struct {
	client *ssm.Client
	prefix string
}

func (s *ssmTokenSource) Token(ctx context.Context, tenant string) (string, error) {
	name := fmt.Sprintf("%s/%s/auth-token", s.prefix, tenant)
	withDecryption := true
	out, err := s.client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           &name,
		WithDecryption: &withDecryption,
	})
	if err != nil {
		var notFound *types.ParameterNotFound
		if errors.As(err, &notFound) {
			return "", errTokenNotFound
		}
		return "", err
	}
	if out.Parameter == nil || out.Parameter.Value == nil {
		return "", errTokenNotFound
	}
	return *out.Parameter.Value, nil
}

type cacheEntry struct {
	token   string
	miss    bool // negative cache: tenant has no token
	expires time.Time
}

type authorizer struct {
	tokens   tokenSource
	cacheTTL time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// expectedToken returns the tenant's token, consulting the cache when a TTL is
// set. Unknown tenants are negative-cached for the same TTL so garbage slugs
// can't hammer the backing store.
func (a *authorizer) expectedToken(ctx context.Context, tenant string) (string, error) {
	if a.cacheTTL > 0 {
		a.mu.Lock()
		e, ok := a.cache[tenant]
		a.mu.Unlock()
		if ok && time.Now().Before(e.expires) {
			if e.miss {
				return "", errTokenNotFound
			}
			return e.token, nil
		}
	}

	tok, err := a.tokens.Token(ctx, tenant)
	if err != nil && !errors.Is(err, errTokenNotFound) {
		// Transient backend failure: fail closed but don't cache.
		return "", err
	}
	if a.cacheTTL > 0 {
		a.mu.Lock()
		a.cache[tenant] = cacheEntry{token: tok, miss: err != nil, expires: time.Now().Add(a.cacheTTL)}
		a.mu.Unlock()
	}
	return tok, err
}

func (a *authorizer) handle(ctx context.Context, req events.APIGatewayCustomAuthorizerRequest) (events.APIGatewayCustomAuthorizerResponse, error) {
	tenant, err := parseTenant(req.MethodArn)
	if err != nil {
		slog.Warn("authorizer: rejecting request", "reason", err.Error())
		return events.APIGatewayCustomAuthorizerResponse{}, errUnauthorized
	}

	provided, ok := strings.CutPrefix(req.AuthorizationToken, "Bearer ")
	if !ok {
		return events.APIGatewayCustomAuthorizerResponse{}, errUnauthorized
	}
	provided = strings.TrimSpace(provided)

	expected, err := a.expectedToken(ctx, tenant)
	if err != nil {
		if !errors.Is(err, errTokenNotFound) {
			slog.Error("authorizer: token lookup failed", "tenant", tenant, "error", err.Error())
		}
		return events.APIGatewayCustomAuthorizerResponse{}, errUnauthorized
	}

	if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		return events.APIGatewayCustomAuthorizerResponse{}, errUnauthorized
	}

	return events.APIGatewayCustomAuthorizerResponse{
		PrincipalID: "adapty-webhook-" + tenant,
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
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	prefix := os.Getenv("TOKEN_PARAM_PREFIX")
	if prefix == "" {
		slog.Error("TOKEN_PARAM_PREFIX env var not set")
		os.Exit(1)
	}

	cacheTTL := 15 * time.Second
	if v := os.Getenv("TOKEN_CACHE_TTL_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs < 0 {
			slog.Error("invalid TOKEN_CACHE_TTL_SECONDS", "value", v)
			os.Exit(1)
		}
		cacheTTL = time.Duration(secs) * time.Second
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("failed to load AWS config", "error", err.Error())
		os.Exit(1)
	}

	a := &authorizer{
		tokens:   &ssmTokenSource{client: ssm.NewFromConfig(cfg), prefix: prefix},
		cacheTTL: cacheTTL,
		cache:    map[string]cacheEntry{},
	}
	slog.Info("authorizer: initialized", "param_prefix", prefix, "cache_ttl", cacheTTL.String())
	lambda.Start(a.handle)
}
