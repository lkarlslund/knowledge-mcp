// Package dashboard provides the local HTTP inspection and maintenance UI.
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/lkarlslund/wikipedia-multistream-mcp/internal/model"
)

//go:embed index.html
var indexHTML []byte

type Service interface {
	ListAvailable(context.Context, string, int, int, bool) (model.AvailableResult, error)
	ListLocal() ([]model.LocalWiki, error)
	ListUpgrades(context.Context) ([]model.OnlineWiki, error)
	ListJobs() []model.Job
	Submit(string, string) (model.Job, error)
}

func Handler(service Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/dashboard/local", func(w http.ResponseWriter, _ *http.Request) {
		result, err := service.ListLocal()
		writeJSON(w, result, err)
	})
	mux.HandleFunc("GET /api/dashboard/jobs", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, service.ListJobs(), nil)
	})
	mux.HandleFunc("GET /api/dashboard/upgrades", func(w http.ResponseWriter, r *http.Request) {
		result, err := service.ListUpgrades(r.Context())
		writeJSON(w, result, err)
	})
	mux.HandleFunc("GET /api/dashboard/available", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		offset, _ := strconv.Atoi(query.Get("offset"))
		limit, _ := strconv.Atoi(query.Get("limit"))
		result, err := service.ListAvailable(r.Context(), query.Get("filter"), offset, limit, query.Get("refresh") == "true")
		writeJSON(w, result, err)
	})
	mux.HandleFunc("POST /api/dashboard/wiki/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Wikipedia-MCP") != "1" {
			writeJSONStatus(w, nil, errors.New("missing maintenance request header"), http.StatusForbidden)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/dashboard/wiki/"), "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] != "download" && parts[1] != "update" {
			writeJSONStatus(w, nil, errors.New("expected /api/dashboard/wiki/{wiki}/{download|update}"), http.StatusBadRequest)
			return
		}
		job, err := service.Submit(parts[0], parts[1])
		writeJSON(w, job, err)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, value any, err error) {
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadRequest
	}
	writeJSONStatus(w, value, err, status)
}

func writeJSONStatus(w http.ResponseWriter, value any, err error, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}
