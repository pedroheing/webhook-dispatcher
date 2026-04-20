package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"webhook-dispatcher/internal/pkg/env"
)

type Config struct {
	APIPort int `env:"API_PORT_CLIENT,required"`
}

func main() {
	config, err := env.Load[Config]()
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		log.Printf("[OK] received webhook: %s", string(body))
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/error", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		log.Printf("[FAIL] received webhook: %s", string(body))
		w.WriteHeader(http.StatusInternalServerError)
	})

	http.HandleFunc("/poison", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		log.Printf("[POISON] received webhook: %s", string(body))
		w.WriteHeader(http.StatusNotFound)
	})

	log.Printf("client listening on %d", config.APIPort)
	http.ListenAndServe(fmt.Sprintf(":%d", config.APIPort), nil)
}
