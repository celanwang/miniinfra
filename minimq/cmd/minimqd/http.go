package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/celanwang/miniinfra/minimq"
)

type registerRequest struct {
	Subscription string
	Config       minimq.SubscriptionConfig
}

type idRequest struct{ ID string }

type historyRequest struct {
	Offset uint64
	Limit  minimq.Limits
}

type pullRequest struct {
	Subscription, Worker string
	Limit                minimq.Limits
}

type deliveryRequest struct {
	Subscription, Worker, Instance, Token, Reason string
}

type deadRequest struct{ Subscription, ID string }

type listDeadRequest struct {
	Subscription, After string
	Limit               int
}

type redriveRequest struct{ Subscription, DeadLetterID, OperationID string }

func routes(p *minimq.Partition) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		result, err := p.Status()
		respond(w, result, err)
	})
	mux.HandleFunc("POST /v1/publish", action(p.Publish))
	mux.HandleFunc("POST /v1/publish/status", action(func(r idRequest) (minimq.PublishResult, error) {
		return p.PublishStatus(r.ID)
	}))
	mux.HandleFunc("POST /v1/history/read", action(func(r historyRequest) (minimq.HistoryPage, error) {
		return p.Read(r.Offset, r.Limit)
	}))
	mux.HandleFunc("POST /v1/subscriptions/register", action(func(r registerRequest) (struct{}, error) {
		return struct{}{}, p.Register(r.Subscription, r.Config)
	}))
	mux.HandleFunc("POST /v1/deliveries/pull", action(func(r pullRequest) ([]minimq.Delivery, error) {
		return p.Pull(r.Subscription, r.Worker, r.Limit)
	}))
	mux.HandleFunc("POST /v1/deliveries/ack", action(func(r deliveryRequest) (minimq.AckResult, error) {
		return p.Ack(r.Subscription, r.Worker, r.Instance, r.Token)
	}))
	mux.HandleFunc("POST /v1/deliveries/fail", action(func(r deliveryRequest) (minimq.FailureResult, error) {
		return p.Fail(r.Subscription, r.Worker, r.Instance, r.Token, r.Reason)
	}))
	mux.HandleFunc("POST /v1/dead-letters/get", action(func(r deadRequest) (minimq.DeadLetter, error) {
		return p.GetDeadLetter(r.Subscription, r.ID)
	}))
	mux.HandleFunc("POST /v1/dead-letters/list", action(func(r listDeadRequest) ([]minimq.DeadLetter, error) {
		return p.ListDeadLetters(r.Subscription, r.After, r.Limit)
	}))
	mux.HandleFunc("POST /v1/dead-letters/redrive", action(func(r redriveRequest) (minimq.RedriveResult, error) {
		return p.Redrive(r.Subscription, r.DeadLetterID, r.OperationID)
	}))
	return mux
}

func action[Request, Result any](run func(Request) (Result, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Request
		r.Body = http.MaxBytesReader(w, r.Body, 2*minimq.MaxMessageBytes+4096)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			respond(w, nil, minimq.ErrInvalid)
			return
		}
		if decoder.Decode(new(json.RawMessage)) != io.EOF {
			respond(w, nil, minimq.ErrInvalid)
			return
		}
		result, err := run(req)
		respond(w, result, err)
	}
}

type errorResponse struct {
	Code  string
	Error string
	First *uint64
}

func respond(w http.ResponseWriter, result any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err == nil {
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	body := errorResponse{Code: "internal", Error: err.Error()}
	status := http.StatusInternalServerError
	var gap *minimq.OffsetGap
	switch {
	case errors.As(err, &gap):
		status, body.Code, body.First = http.StatusGone, "offset_gap", &gap.First
	case errors.Is(err, minimq.ErrInvalid):
		status, body.Code = http.StatusBadRequest, "invalid"
	case errors.Is(err, minimq.ErrConflict):
		status, body.Code = http.StatusConflict, "conflict"
	case errors.Is(err, minimq.ErrCapacity):
		status, body.Code = http.StatusServiceUnavailable, "capacity"
	case errors.Is(err, minimq.ErrClockUncertain):
		status, body.Code = http.StatusServiceUnavailable, "clock_uncertain"
	case errors.Is(err, minimq.ErrSubscription), errors.Is(err, minimq.ErrNotFound):
		status, body.Code = http.StatusNotFound, "not_found"
	case errors.Is(err, minimq.ErrLeaseExpired):
		status, body.Code = http.StatusConflict, "lease_expired"
	case errors.Is(err, minimq.ErrOutcomeUnknown):
		status, body.Code = http.StatusConflict, "outcome_unknown"
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
