package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func main() {
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

	fmt.Println("receiver listening on :8090")
	http.ListenAndServe(":8090", nil)
}
