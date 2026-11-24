// Binario de dashboard: solo lee ClickHouse. Nunca decide si un pago está cobrado.
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/patricioarmesto/0007-stripe/internal/config"
	"github.com/patricioarmesto/0007-stripe/internal/dashboard"
	"github.com/patricioarmesto/0007-stripe/internal/report"
)

func main() {
	cfg := config.Load()
	store, err := report.Open(cfg.ClickHouseDSN)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           dashboard.NewHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("dashboard escuchando en :%s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
