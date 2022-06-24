package node

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (n *DBNode) addAdminEndpoints(router *mux.Router) {
	group := router.PathPrefix("/raft/admin")

	group.Path("/campaign").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := n.RaftNode.Campaign(); err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		_, _ = w.Write([]byte("ok"))
	}).Methods("POST")
}
