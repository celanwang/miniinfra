package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/celanwang/miniinfra/minimq"
)

func call[Response any](t *testing.T, handler http.Handler, path string, request any, code int) Response {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
	if w.Code != code {
		t.Fatalf("%s status = %d, want %d. Body: %s", path, w.Code, code, w.Body.String())
	}
	var result Response
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHTTPPublishReadPullAndAckAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	handler := routes(p)
	limit := minimq.Limits{Messages: 10, Bytes: 4096}
	call[struct{}](t, handler, "/v1/subscriptions/register", registerRequest{Subscription: "notify"}, http.StatusOK)
	req := minimq.PublishRequest{ID: "order-shipped", Body: []byte("order 731")}
	pub := call[minimq.PublishResult](t, handler, "/v1/publish", req, http.StatusOK)
	page := call[minimq.HistoryPage](t, handler, "/v1/history/read", historyRequest{Limit: limit}, http.StatusOK)
	if page.Next != pub.Offset+1 || len(page.Messages) != 1 || !bytes.Equal(page.Messages[0].Body, req.Body) {
		t.Fatalf("unexpected history: %+v", page)
	}
	pull := pullRequest{Subscription: "notify", Worker: "sender", Limit: limit}
	deliveries := call[[]minimq.Delivery](t, handler, "/v1/deliveries/pull", pull, http.StatusOK)
	if len(deliveries) != 1 || deliveries[0].Attempt != 1 {
		t.Fatalf("history read changed delivery state: %+v", deliveries)
	}
	ack := deliveryRequest{Subscription: "notify", Worker: "sender", Instance: deliveries[0].Instance, Token: deliveries[0].Token}
	if got := call[minimq.AckResult](t, handler, "/v1/deliveries/ack", ack, http.StatusOK); got != minimq.AckAccepted {
		t.Fatalf("ACK = %q", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	handler = routes(reopened)
	if got := call[minimq.AckResult](t, handler, "/v1/deliveries/ack", ack, http.StatusOK); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("recovered ACK = %q", got)
	}
	if got := call[[]minimq.Delivery](t, handler, "/v1/deliveries/pull", pull, http.StatusOK); len(got) != 0 {
		t.Fatalf("ACKed message was redelivered: %+v", got)
	}
}
