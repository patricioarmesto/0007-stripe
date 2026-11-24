// Package cache existe por una razón puntual: dos requests con la misma clave
// pueden llegar al mismo tiempo y los dos encontrar "no existe" en Postgres.
// El lock de Redis gana esa carrera de milisegundos. No es la verdad: la verdad
// sigue siendo el UNIQUE de Postgres.
package cache

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Locker interface {
	Acquire(ctx context.Context, key string) (string, bool, error)
	Release(ctx context.Context, key, token string) error
}

type RedisLocker struct{ client *redis.Client }

func NewRedisLocker(client *redis.Client) *RedisLocker { return &RedisLocker{client: client} }

func (l *RedisLocker) Acquire(ctx context.Context, key string) (string, bool, error) {
	token := uuid.NewString()
	// NX = solo si no existe; PX 10000 = expira en 10s para que un proceso
	// caído no deje el pago trabado para siempre.
	ok, err := l.client.SetNX(ctx, key, token, 10*time.Second).Result()
	return token, ok, err
}

var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end`)

func (l *RedisLocker) Release(ctx context.Context, key, token string) error {
	return releaseScript.Run(ctx, l.client, []string{key}, token).Err()
}
