package server

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/antekresic/grs/domain"
	"github.com/julienschmidt/httprouter"
)

//HTTP is a HTTP server that will handle incoming requests
type HTTP struct {
	router    http.Handler
	Repo      domain.EntryRepository
	Validator domain.EntryValidator
}

func (s *HTTP) setRouter() {
	router := httprouter.New()

	router.Handle("POST", "/entry", s.handleNewEntry)
	s.router = router
}

func (s HTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if s.router == nil {
		s.setRouter()
	}

	s.router.ServeHTTP(w, req)
}

func (s HTTP) handleNewEntry(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)

	if err != nil {
		log.Printf("Error reading request body: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var e domain.Entry
	err = json.Unmarshal(body, &e)
	if err != nil {
		log.Printf("Error unmarshaling body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Validator.Validate(e)
	if err != nil {
		log.Printf("Error validating entry: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Repo.AddEntry(e)
	if err != nil {
		log.Printf("Error adding entry to repo: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
