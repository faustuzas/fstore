package node

import (
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	_ "net/http/pprof"
)

func (n *DBNode) addObservabilityEndpoints(router *mux.Router) {
	// attaches all required endpoints
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	router.PathPrefix("/metrics").Handler(promhttp.Handler())
}
