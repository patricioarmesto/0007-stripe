// Binario de pagos: solo HTTP. No conoce nada de ClickHouse ni de webhooks.
package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/cache"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/payments"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	repo := payments.NewSQLRepository(db)
	svc := payments.NewService(repo, cache.NewRedisLocker(redisClient), events.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic))
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           payments.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("payments escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
