package handler

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

type Queue struct {
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	BacklogDepth   int     `json:"backlogDepth"`
	DLQCount       int     `json:"dlqCount"`
	IngressPerMin  float64 `json:"ingressPerMin"`
	WorkerCount    int     `json:"workerCount"`
	AvgProcessMs   float64 `json:"avgProcessMs"`
	FailureRatePct float64 `json:"failureRatePct"`
	MaxRetries     int     `json:"maxRetries"`
	Status         string  `json:"status"`
}

var (
	queueMu sync.Mutex
	queues = map[string]*Queue{
		"mpesa-stk-callbacks": {ID:"mpesa-stk-callbacks",Name:"M-Pesa STK callbacks",BacklogDepth:1420,DLQCount:18,IngressPerMin:240,WorkerCount:8,AvgProcessMs:800,FailureRatePct:4,MaxRetries:4,Status:"LAGGING"},
		"webhook-outbound-relay": {ID:"webhook-outbound-relay",Name:"Webhook outbound relay",BacklogDepth:286,DLQCount:64,IngressPerMin:110,WorkerCount:12,AvgProcessMs:420,FailureRatePct:13,MaxRetries:5,Status:"DLQ_SPIKE"},
		"ledger-report-worker": {ID:"ledger-report-worker",Name:"Ledger report worker",BacklogDepth:94,DLQCount:2,IngressPerMin:35,WorkerCount:4,AvgProcessMs:1200,FailureRatePct:1,MaxRetries:3,Status:"HEALTHY"},
	}
)

func classify(q *Queue) string {
	if q.DLQCount > maxInt(20, int(float64(q.BacklogDepth)*0.2)) { return "DLQ_SPIKE" }
	if q.BacklogDepth > int(q.IngressPerMin*3) { return "LAGGING" }
	return "HEALTHY"
}
func maxInt(a,b int) int { if a>b{return a};return b }
func queueList() []Queue { out:=make([]Queue,0,len(queues));for _,q:=range queues{out=append(out,*q)};return out }

func Handler(w http.ResponseWriter,r *http.Request) {
	if strings.TrimSuffix(r.URL.Path, "/") == "/api/step" { stepHandler(w,r); return }
	w.Header().Set("Content-Type","application/json")
	w.Header().Set("Cache-Control","no-store")
	if r.Method=="GET" { queueMu.Lock();defer queueMu.Unlock();_ = json.NewEncoder(w).Encode(map[string]any{"queues":queueList()});return }
	if r.Method!="POST" { w.Header().Set("Allow","GET, POST");http.Error(w,`{"error":"method not allowed"}`,http.StatusMethodNotAllowed);return }
	var body struct { Action string `json:"action"`; Name string `json:"name"`; QueueID string `json:"queueId"` }
	if err:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1<<20)).Decode(&body);err!=nil { http.Error(w,`{"error":"invalid JSON body"}`,http.StatusBadRequest);return }
	queueMu.Lock();defer queueMu.Unlock()
	switch strings.ToLower(body.Action) {
	case "create":
		name:=strings.TrimSpace(body.Name);if name==""||len(name)>48 { http.Error(w,`{"error":"name must contain 1–48 characters"}`,http.StatusBadRequest);return }
		id:=strings.ToLower(strings.ReplaceAll(name," ","-"));if _,ok:=queues[id];ok { http.Error(w,`{"error":"queue already exists"}`,http.StatusConflict);return }
		queues[id]=&Queue{ID:id,Name:name,IngressPerMin:60,WorkerCount:4,AvgProcessMs:500,FailureRatePct:2,MaxRetries:3,Status:"HEALTHY"}
	case "reset":
		q,ok:=queues[body.QueueID];if !ok { http.Error(w,`{"error":"queue not found"}`,http.StatusNotFound);return };q.BacklogDepth=0;q.DLQCount=0;q.Status="HEALTHY"
	default: http.Error(w,`{"error":"action must be create or reset"}`,http.StatusBadRequest);return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"queues":queueList()})
}
