package main

import (
	"log"
	"net/http"
	"os"
	"os/exec"
)

func main() {
	http.HandleFunc("/", scriptHandler)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("Listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func scriptHandler(w http.ResponseWriter, r *http.Request) {
	cmd := exec.CommandContext(r.Context(), "python", "test1.py")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		w.WriteHeader(500)
	}
	w.Write(out)
}

