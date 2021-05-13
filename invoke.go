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
	log.Printf("step1")
	cmd := exec.CommandContext(r.Context(), "/opt/conda/bin/python", "test1.py")
	log.Printf("step2")
	cmd.Stderr = os.Stderr
	log.Printf("step3")
	out, err := cmd.Output()
	log.Printf("step4")
	if err != nil {
		log.Printf("step5")
		w.WriteHeader(500)
	}
	log.Printf("step6")
	w.Write(out)
}

