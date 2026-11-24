package cache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLockNXYLiberacionConToken(t *testing.T) {
	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	locker := NewRedisLocker(client)
	ctx := context.Background()

	token, ok, err := locker.Acquire(ctx, "idem:abc")
	if err != nil || !ok || token == "" {
		t.Fatalf("primero debía adquirir: %q %v %v", token, ok, err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); ok {
		t.Fatal("el segundo NO debe adquirir el mismo lock")
	}
	if err := locker.Release(ctx, "idem:abc", "token-ajeno"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); ok {
		t.Fatal("no se puede liberar con el token de otro")
	}
	if err := locker.Release(ctx, "idem:abc", token); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := locker.Acquire(ctx, "idem:abc"); !ok {
		t.Fatal("después de liberar con el token correcto sí debe adquirir")
	}
}
