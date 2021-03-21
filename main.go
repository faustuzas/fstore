package main

import (
	"context"
	"fmt"
	"github.com/faustuzas/fstore/store"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

type HandlerFunc func (http.ResponseWriter, *http.Request)

func handleServerError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("error while handling %s %s: %v", r.Method, r.RequestURI, err)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func requestLogger(inner HandlerFunc) HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		inner(w, r)
		duration := time.Since(start)
		log.Printf("%s %s [%v]", r.Method, r.RequestURI, duration)
	}
}

func putKeyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		handleServerError(w, r, err)
		return
	}

	err = store.Put(key, string(value))
	if err != nil {
		handleServerError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func getKeyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, ok, err := store.Get(key)
	if err != nil {
		handleServerError(w, r, err)
		return
	}

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		if _, err := w.Write([]byte(fmt.Sprintf("Entry with key '%s' not found", key))); err != nil {
			handleServerError(w, r, err)
		}
		return
	}

	_, err = w.Write([]byte(value))
	if err != nil {
		handleServerError(w, r, err)
	}
}

func deleteKeyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := store.Delete(key)
	if err != nil {
		handleServerError(w, r, err)
		return
	}
}

func initServer(r *mux.Router) (*http.Server, *sync.WaitGroup) {
	var srv = &http.Server{Addr: ":8082", Handler: r}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		log.Printf("starting server")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal("cannot start server: ", err)
		}
	}()

	return srv, &wg
}

func registerRoutes() *mux.Router {
	router := mux.NewRouter()

	middleware := requestLogger

	router.HandleFunc("/v1/{key}", middleware(getKeyHandler)).Methods("GET")
	router.HandleFunc("/v1/{key}", middleware(putKeyHandler)).Methods("PUT")
	router.HandleFunc("/v1/{key}", middleware(deleteKeyHandler)).Methods("DELETE")

	return router
}

func onShutdown(callback func ()) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		<- c
		log.Println("running shutdown hook")
		callback()
	}()

	log.Println("shutdown hook registered")
}

func main() {
	router := registerRoutes()
	server, wg := initServer(router)

	onShutdown(func() {
		if err := store.Done(); err != nil {
			log.Printf("error while shutting down store: %v\n", err)
		}

		log.Printf("shutting down server")
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("error while shutting down server: %v\n", err)
		}
		wg.Done()
	})
	wg.Wait()
}
