package insight

import "context"

// v1 has no authentication on /api/v1 or MCP (02 §10). The types below are
// the seams that a later release fills, so adding authentication changes no
// operation and no tool signature (section 6.4 of the agent-access plan).

// CredentialSource returns the Authorization header value for a request, or
// an empty string for none.
type CredentialSource interface {
	Credential(ctx context.Context) (string, error)
}

type credentialKey struct{}

// WithCredential returns a context that carries the caller's Authorization
// header value, such as "Bearer <token>". An MCP server puts each caller's own
// token here, so query can authorize the end user.
func WithCredential(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, credentialKey{}, value)
}

// ContextCredential reads the credential WithCredential put in the context.
type ContextCredential struct{}

// Credential returns the context's credential, or an empty string.
func (ContextCredential) Credential(ctx context.Context) (string, error) {
	value, _ := ctx.Value(credentialKey{}).(string)
	return value, nil
}

// Authorizer decides whether the caller may read the named namespaces. An
// operation calls it with the namespaces its conditions name; an empty list
// means the query names none and reads across namespaces.
type Authorizer func(ctx context.Context, namespaces []string) error

// AllowAll is the v1 Authorizer: every caller may read every namespace.
func AllowAll(context.Context, []string) error { return nil }
