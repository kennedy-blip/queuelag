package handler

import (
	"encoding/json"
	"math"
	"net/http"
	"sync"
)

type StepRequest struct {
	QueueID string `json:"queueId"`
	Steps int `json:"steps"`
	IngressPerMin float64 `json:"ingressPerMin"`
	WorkerCount int `json:"workerCount"`
	AvgProcessMs float64 `json:"avgProcessMs"`
	FailureRatePct float64 `json:"failureRatePct"`
	MaxRetries int `json:"maxRetries"`
	DrainDLQ bool `json:"drainDlq"`
}
type Telemetry struct {
	Step int `json:"step"`
	BacklogDepth int `json:"backlogDepth"`
	Completed int `json:"completed"`
	Retried int `json:"retried"`
	DeadLettered int `json:"deadLettered"`
	Ingress float64 `json:"ingress"`
	Throughput float64 `json:"throughput"`
}
func clamp(v,lo,hi float64) float64 { return math.Max(lo,math.Min(hi,v)) }

func stepHandler(w http.ResponseWriter,r *http.Request) {
	w.Header().Set("Content-Type","application/json");w.Header().Set("Cache-Control","no-store")
	if r.Method!="POST" { w.Header().Set("Allow","POST");http.Error(w,`{"error":"method not allowed"}`,http.StatusMethodNotAllowed);return }
	var req StepRequest
	if err:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1<<20)).Decode(&req);err!=nil { http.Error(w,`{"error":"invalid JSON body"}`,http.StatusBadRequest);return }
	if req.Steps<1||req.Steps>30||req.WorkerCount<1||req.WorkerCount>256||req.AvgProcessMs<=0||req.AvgProcessMs>60000||req.IngressPerMin<0||req.IngressPerMin>1000000||req.FailureRatePct<0||req.FailureRatePct>100||req.MaxRetries<0||req.MaxRetries>20 { http.Error(w,`{"error":"simulation parameters are outside supported limits"}`,http.StatusBadRequest);return }

	queueMu.Lock()
	q,ok:=queues[req.QueueID]
	if !ok { queueMu.Unlock();http.Error(w,`{"error":"queue not found"}`,http.StatusNotFound);return }
	q.IngressPerMin=req.IngressPerMin;q.WorkerCount=req.WorkerCount;q.AvgProcessMs=req.AvgProcessMs;q.FailureRatePct=req.FailureRatePct;q.MaxRetries=req.MaxRetries
	backlog,dlq:=float64(q.BacklogDepth),float64(q.DLQCount)
	queueMu.Unlock()

	series:=make([]Telemetry,req.Steps)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i:=0;i<req.Steps;i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			mu.Lock();defer mu.Unlock()
			incoming:=req.IngressPerMin
			capacity:=float64(req.WorkerCount)*60000/req.AvgProcessMs
			available:=backlog+incoming
			attempts:=math.Min(available,capacity)
			failed:=attempts*req.FailureRatePct/100
			completed:=attempts-failed
			retryable:=failed
			dead:=0.0
			for retry:=0;retry<=req.MaxRetries && retryable>0;retry++ {
				next:=retryable*req.FailureRatePct/100
				if retry==req.MaxRetries { dead=next } else { retryable=next }
			}
			retried:=failed-dead
			backlog=math.Max(0,available-completed-dead)
			dlq+=dead
			if req.DrainDLQ&&dlq>0 { replay:=math.Min(dlq,math.Max(1,float64(req.WorkerCount)*2));dlq-=replay;backlog+=replay }
			series[index]=Telemetry{Step:index+1,BacklogDepth:int(math.Round(backlog)),Completed:int(math.Round(completed)),Retried:int(math.Round(retried)),DeadLettered:int(math.Round(dead)),Ingress:incoming,Throughput:completed}
		}(i)
	}
	wg.Wait()
	queueMu.Lock()
	q.BacklogDepth=int(math.Round(backlog));q.DLQCount=int(math.Round(dlq));q.Status=classify(q)
	updated:=*q
	queueMu.Unlock()
	capacity:=float64(req.WorkerCount)*60000/req.AvgProcessMs*(1-req.FailureRatePct/100)
	net:=capacity-req.IngressPerMin
	drain:=0.0
	if net>0 { drain=float64(updated.BacklogDepth)/net*60 } else { drain=-1 }
	_ = json.NewEncoder(w).Encode(map[string]any{"queue":updated,"series":series,"timeToDrainSec":drain})
}
