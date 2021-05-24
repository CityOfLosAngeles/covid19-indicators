package main

import (
	"strings"
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
	COMMAND:=os.Getenv("COMMAND")
	RUN_SCRIPT:=os.Getenv("RUN_SCRIPT")
	if COMMAND == "" {
		cmd := exec.CommandContext(r.Context(), "python", "gcp/test1.py")
		cmd.Stderr = os.Stderr
		out, err := cmd.Output()
		if err != nil {
			w.WriteHeader(500)
		}
		w.Write(out)
	} else if strings.Contains(COMMAND,"bash") {
		cmd := exec.CommandContext(r.Context(), COMMAND,"-c", RUN_SCRIPT)
		cmd.Stderr = os.Stderr
		out, err := cmd.Output()
		if err != nil {
			w.WriteHeader(500)
		}
		w.Write(out)
	}
}

