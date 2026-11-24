// Binario de webhook: recibe los avisos del banco (simulados localmente).
package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/events"
	"github.com/patricioarmesto/0007-stripe/internal/webhook"
)

func main() {
	cfg := config.Load()
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	repo := webhook.NewSQLRepository(db)
	svc := webhook.NewService(repo, events.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic))
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           webhook.NewHandler(svc, cfg.WebhookSecret),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("webhook escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
