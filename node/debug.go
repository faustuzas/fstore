package node

import (
	"github.com/gorilla/mux"
	"net/http"
	_ "net/http/pprof"
)

func attachProfiler(router *mux.Router) {
	// attaches all required endpoints
	router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
}
