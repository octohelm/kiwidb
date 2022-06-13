package id

import "context"

type contextKey struct{}

func FromContext(ctx context.Context) Gen {
	return ctx.Value(contextKey{}).(Gen)
}

func InjectContext(ctx context.Context, gen Gen) context.Context {
	return context.WithValue(ctx, contextKey{}, gen)
}
