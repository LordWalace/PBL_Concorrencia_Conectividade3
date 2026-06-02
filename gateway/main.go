package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MsgRequest          = "REQUEST"
	MsgReply            = "REPLY"
	MsgRelease          = "RELEASE"
	MsgHeartbeat        = "HEARTBEAT"
	MsgHeartbeatAck     = "HEARTBEAT_ACK"
	MsgDroneFailed      = "DRONE_FAILED"
	MsgSnapshotRequest  = "SNAPSHOT_REQUEST"
	MsgStateSync        = "STATE_SYNC"
	MsgAlert            = "ALERT"
	MsgAlertClaim       = "ALERT_CLAIM"
	MsgDeviceReg        = "DEVICE_REG"
	MsgStatusReq        = "STATUS_REQ"
	MsgEventsReq        = "EVENTS_REQ"
	MsgEventsRep        = "EVENTS_REP"
	MsgStatusRep        = "STATUS_REP"
	MsgPeerHeartbeat    = "PEER_HEARTBEAT"
	MsgPeerHeartbeatAck = "PEER_HEARTBEAT_ACK"
	MsgAck              = "MSG_ACK"
)

const (
	DroneAvailable = "DISPONIVEL"
	DroneBusy      = "OCUPADO"
	DroneFailed    = "FALHO"
)

type Message struct {
	Type        string            `json:"type"`
	DroneID     string            `json:"drone_id,omitempty"`
	GatewayID   string            `json:"gateway_id,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Lamport     int               `json:"lamport,omitempty"`
	Timestamp   int64             `json:"timestamp,omitempty"`
	Payload     map[string]string `json:"payload,omitempty"`
	Content     string            `json:"content,omitempty"`
	Status      string            `json:"status,omitempty"`
	MissionInfo string            `json:"mission_info,omitempty"`
	Occurrence  string            `json:"occurrence,omitempty"`
	Queue       []AlertRequest    `json:"queue,omitempty"`
}

type DroneState struct {
	ID            string
	Status        string
	GatewayAtual  string
	ControlAddr   string
	MissionActive bool
	MissionInfo   string
	LastHeartbeat time.Time
	SetorBase     string
	LastUpdate    time.Time
}

type AlertRequest struct {
	RequestID          string `json:"request_id"`
	Occurrence         string `json:"occurrence"`
	Priority           int    `json:"priority"`
	Lamport            int    `json:"lamport"`
	GatewayID          string `json:"gateway_id"`
	Timestamp          int64  `json:"timestamp"`
	RetryCount         int    `json:"retry_count,omitempty"`
	SuspendedUntilUnix int64  `json:"suspended_until,omitempty"`
}

type PriorityQueue []*AlertRequest

// Len retorna a quantidade de itens no heap, garantindo a manutencao da estrutura de prioridade para selecao de alertas distribuido.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less define a ordem de prioridade considerando prioridade, relogio Lamport, timestamp e ID de gateway para decisao deterministica.
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	if pq[i].Lamport != pq[j].Lamport {
		return pq[i].Lamport < pq[j].Lamport
	}
	if pq[i].Timestamp != pq[j].Timestamp {
		return pq[i].Timestamp < pq[j].Timestamp
	}
	return pq[i].GatewayID < pq[j].GatewayID
}

// Swap troca elementos do heap preservando a consistencia da fila de prioridade distribuida.
func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

// Push insere um alerta no heap de prioridades mantendo a invariante da fila.
func (pq *PriorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*AlertRequest)) }

// Pop remove o proximo alerta de maior prioridade do heap de forma segura.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // evita memory leak ao remover o item
	*pq = old[:n-1]
	return item
}

// nextReadyRequestIndex seleciona o proximo alerta elegivel para processamento, ignorando backoff temporario.
func nextReadyRequestIndex() int {
	now := time.Now()
	best := -1
	for i, req := range reqQueue {
		if req.SuspendedUntilUnix != 0 && now.Before(time.Unix(0, req.SuspendedUntilUnix)) {
			continue
		}
		if best == -1 || reqQueue.Less(i, best) {
			best = i
		}
	}
	return best
}

var (
	gatewayID             string
	gatewayIP             string
	gatewayHost           string
	regPort               string
	clientPort            string
	peerPort              string
	peerAddrsByID         map[string]string
	peers                 []string
	peerIDs               []string
	peerOfflineUntil      = make(map[string]time.Time)
	peerFailureCount      = make(map[string]int)
	replyChannels         = make(map[string]map[string]chan struct{})
	replyChannelMutex     sync.Mutex
	queueNotify           = make(chan struct{}, 1)
	lamportClock          int
	lamportMutex          sync.Mutex
	stateMutex            sync.Mutex
	drones                = make(map[string]*DroneState)
	activeBeacons         = make(map[string]time.Time)
	droneOwners           = make(map[string]string)
	droneUnavailableUntil = make(map[string]time.Time)
	deferred              = make(map[string][]Message)
	repliesCount          = make(map[string]int)
	requestingCS          = make(map[string]bool)
	myCurrentReq          = make(map[string]Message)
	currentReqRetries     = make(map[string]int)
	reqQueue              PriorityQueue
	seenAlerts            = make(map[string]bool)
	claimedAlerts         = make(map[string]bool)
	eventLog              []string
	eventMutex            sync.Mutex
)

// mustEnv valida que a variavel de ambiente exista antes da inicializacao do servico.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("[FATAL] Variável de ambiente obrigatória ausente: %s", key)
	}
	return v
}

// normalizeDroneID normaliza o identificador do drone para nomes estaveis usados em logs e relatorios.
func normalizeDroneID(droneID string) string {
	if strings.HasPrefix(droneID, "Drone_") {
		droneID = strings.TrimPrefix(droneID, "Drone_")
	} else if strings.HasPrefix(droneID, "drone_") {
		droneID = strings.TrimPrefix(droneID, "drone_")
	}
	if idx := strings.LastIndex(droneID, "_"); idx != -1 {
		if _, err := strconv.Atoi(droneID[idx+1:]); err == nil {
			droneID = droneID[:idx]
		}
	}
	return strings.ReplaceAll(droneID, "_", "-")
}

// queueStateFilePath retorna o caminho do arquivo local usado para persistir o estado da fila de alertas.
func queueStateFilePath() string {
	return fmt.Sprintf("%s_queue_state.json", gatewayID)
}

// loadQueueState restaura o estado da fila de alertas apos reinicializacao para evitar perda de dados.
func loadQueueState() {
	path := queueStateFilePath()
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var saved []*AlertRequest
	if err := json.Unmarshal(data, &saved); err != nil {
		log.Printf("[GATEWAY/%s] Falha ao carregar estado da fila: %v", gatewayID, err)
		return
	}
	stateMutex.Lock()
	for _, req := range saved {
		if req.RequestID == "" {
			req.RequestID = fmt.Sprintf("%s:%d:%d", req.GatewayID, req.Timestamp, req.Lamport)
		}
		if seenAlerts[req.RequestID] {
			continue
		}
		seenAlerts[req.RequestID] = true
		reqQueue = append(reqQueue, req)
	}
	heap.Init(&reqQueue)
	stateMutex.Unlock()
	log.Printf("[GATEWAY/%s] Estado da fila restaurado com %d requisições pendentes", gatewayID, reqQueue.Len())
}

// persistQueueStateLocked grava o estado da fila de alertas de forma atomica quando o mutex ja esta travado.
func persistQueueStateLocked() {
	queueCopy := make([]*AlertRequest, 0, reqQueue.Len())
	for _, req := range reqQueue {
		copyReq := *req
		queueCopy = append(queueCopy, &copyReq)
	}
	go func(dataToSave []*AlertRequest) {
		data, err := json.Marshal(dataToSave)
		if err != nil {
			log.Printf("[GATEWAY/%s] Falha ao serializar estado da fila: %v", gatewayID, err)
			return
		}
		tmpPath := fmt.Sprintf("%s.tmp", queueStateFilePath())
		if err := os.WriteFile(tmpPath, data, 0644); err != nil {
			log.Printf("[GATEWAY/%s] Falha ao gravar estado temporário da fila: %v", gatewayID, err)
			return
		}
		if err := os.Rename(tmpPath, queueStateFilePath()); err != nil {
			log.Printf("[GATEWAY/%s] Falha ao atualizar arquivo de estado da fila: %v", gatewayID, err)
		}
	}(queueCopy)
}

// persistQueueState salva o estado atual da fila de alertas com bloqueio para evitar condicoes de corrida.
func persistQueueState() {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	persistQueueStateLocked()
}

// notifyQueueProcessor acorda imediatamente o loop de processamento quando a fila muda de estado.
func notifyQueueProcessor() {
	select {
	case queueNotify <- struct{}{}:
	default:
	}
}

// isDroneUnavailable verifica se o drone esta em backoff e deve ser mantido fora de novas alocacoes temporariamente.
func isDroneUnavailable(droneID string) bool {
	if until, ok := droneUnavailableUntil[droneID]; ok {
		return time.Now().Before(until)
	}
	return false
}

// isRequestClaimed evita que uma requisicao ja assumida retorne a fila ou seja processada duas vezes.
func isRequestClaimed(requestID string) bool {
	return requestID != "" && (seenAlerts[requestID] || claimedAlerts[requestID])
}

// markAlertClaimed registra globalmente que uma requisicao foi assumida por um gateway na malha.
func markAlertClaimed(requestID string) {
	if requestID == "" {
		return
	}
	claimedAlerts[requestID] = true
	seenAlerts[requestID] = true
}

// removeAlertFromQueue extrai uma requisicao da fila quando ela ja foi confirmada por outro gateway.
func removeAlertFromQueue(requestID string) {
	for i, req := range reqQueue {
		if req.RequestID == requestID {
			heap.Remove(&reqQueue, i)
			persistQueueStateLocked()
			return
		}
	}
}

// ensureRequestID atribui identificador unico a mensagens de alerta antes de replicar ou processar.
func ensureRequestID(msg *Message) {
	if msg.RequestID == "" {
		msg.RequestID = fmt.Sprintf("%s:%d:%d", gatewayID, time.Now().UnixNano(), tickLamport(msg.Lamport))
	}
}

// enqueueAlert adiciona alertas locais a fila distribuida e replica o evento para peers, garantindo que nao haja duplicacoes.
func enqueueAlert(msg Message) {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	if msg.GatewayID == "" {
		msg.GatewayID = gatewayID
	}
	ensureRequestID(&msg)
	if isRequestClaimed(msg.RequestID) {
		return
	}
	seenAlerts[msg.RequestID] = true
	req := &AlertRequest{
		RequestID:  msg.RequestID,
		Occurrence: msg.Occurrence,
		Priority:   msg.Priority,
		Lamport:    tickLamport(msg.Lamport),
		GatewayID:  msg.GatewayID,
		Timestamp:  time.Now().Unix(),
	}
	heap.Push(&reqQueue, req)
	persistQueueStateLocked()
	notifyQueueProcessor()
	logEvent(fmt.Sprintf("[R-A] Alerta enfileirado: %s prior. %d", req.Occurrence, req.Priority))
	log.Printf("[GATEWAY/%s] [R-A] Alerta enfileirado: %s prioridade %d", gatewayID, req.Occurrence, req.Priority)
	if msg.GatewayID == gatewayID {
		broadcastPeerMsg(msg)
	}
}

// enqueueAlertFromPeer incorpora alertas recebidos de peers ao estado local sem perder priorizacao.
func enqueueAlertFromPeer(req AlertRequest) {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	if req.RequestID == "" {
		req.RequestID = fmt.Sprintf("%s:%d:%d", req.GatewayID, req.Timestamp, req.Lamport)
	}
	if isRequestClaimed(req.RequestID) {
		return
	}
	seenAlerts[req.RequestID] = true
	heap.Push(&reqQueue, &req)
	persistQueueStateLocked()
	notifyQueueProcessor()
	logEvent(fmt.Sprintf("[R-A] Alerta replicado recebido: %s prior. %d", req.Occurrence, req.Priority))
	log.Printf("[GATEWAY/%s] [R-A] Alerta replicado recebido: %s prioridade %d", gatewayID, req.Occurrence, req.Priority)
}

// mergeQueueFromStateSync unifica o estado de fila recebido de outro gateway com o estado local de forma convergente.
func mergeQueueFromStateSync(queue []AlertRequest) {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	updated := false
	for _, req := range queue {
		if req.RequestID == "" {
			req.RequestID = fmt.Sprintf("%s:%d:%d", req.GatewayID, req.Timestamp, req.Lamport)
		}
		if isRequestClaimed(req.RequestID) {
			continue
		}
		seenAlerts[req.RequestID] = true
		heap.Push(&reqQueue, &req)
		updated = true
	}
	if updated {
		persistQueueStateLocked()
		notifyQueueProcessor()
	}
}

// main inicializa o servico e os componentes de rede, garantindo sincronizacao e redundancia entre gateways.
func main() {
	heap.Init(&reqQueue)
	gatewayID = mustEnv("GATEWAY_ID")
	gatewayIP = mustEnv("GATEWAY_IP")
	gatewayHost = mustEnv("GATEWAY_HOST")
	regPort = mustEnv("GATEWAY_TCP_REG_PORT")
	clientPort = mustEnv("GATEWAY_TCP_CLIENT_PORT")
	peerPort = mustEnv("GATEWAY_TCP_PEER_PORT")

	peerAddrsByID = map[string]string{
		"Norte": fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), peerPort),
		"Sul":   fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), peerPort),
		"Leste": fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), peerPort),
		"Oeste": fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), peerPort),
	}

	loadQueueState()

	for id, addr := range peerAddrsByID {
		if id == gatewayID {
			continue
		}
		peers = append(peers, addr)
		peerIDs = append(peerIDs, id)
	}

	log.Printf("[GATEWAY/%s] Iniciando gateway em %s", gatewayID, gatewayHost)
	log.Printf("[GATEWAY/%s] Peers conhecidos: %v", gatewayID, peers)

	go startServer(gatewayHost, peerPort, handlePeerConnection)
	go startServer(gatewayHost, regPort, handleRegConnection)
	go startServer(gatewayHost, clientPort, handleClientConnection)

	go syncStateOnStart()
	go processQueueLoop()
	go monitorLocalDroneHeartbeats()
	go startPeerHealthMonitor()

	select {}
}

// startServer inicia um servidor TCP concorrente para o tipo de conexao especificado.
func startServer(host, port string, handler func(net.Conn)) {
	addr := fmt.Sprintf("%s:%s", host, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[GATEWAY/%s] Falha ao escutar em %s: %v", gatewayID, addr, err)
	}
	log.Printf("[GATEWAY/%s] Servidor TCP ativo em %s", gatewayID, addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handler(conn)
	}
}

// handlePeerConnection processa mensagens P2P entre gateways e garante ack para protocolo de coordenacao.
func handlePeerConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}
	updateLamport(msg.Lamport)
	switch msg.Type {
	case MsgRequest:
		handleRARequest(msg)
		sendPeerAck(conn, msg)
	case MsgReply:
		handleRAReply(msg)
		sendPeerAck(conn, msg)
	case MsgRelease:
		handleRARelease(msg)
		sendPeerAck(conn, msg)
	case MsgAlert:
		msgCopy := msg
		go enqueueAlert(msgCopy)
		sendPeerAck(conn, msg)
	case MsgSnapshotRequest:
		sendStateSync(conn)
	case MsgStateSync:
		msgCopy := msg
		go receiveStateSync(msgCopy)
	case MsgDroneFailed:
		handleDroneFailed(msg)
		sendPeerAck(conn, msg)
	case MsgAlertClaim:
		handleAlertClaim(msg)
		sendPeerAck(conn, msg)
	case MsgPeerHeartbeat:
		sendPeerAck(conn, msg)
	case MsgPeerHeartbeatAck:
		markPeerOnline(msg.GatewayID)
	}
}

// handleRegConnection trata o registro de drones e heartbeat de dispositivos para manter o estado distribuido.
func handleRegConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}
	updateLamport(msg.Lamport)
	switch msg.Type {
	case MsgDeviceReg:
		handleDeviceRegistration(msg)
	case MsgHeartbeat:
		handleDroneHeartbeat(msg, conn)
	case MsgRelease:
		releaseCS(msg.DroneID, true)
	case MsgAlert:
		enqueueAlert(msg)
	case MsgStatusReq:
		sendStatusRep(conn)
	case MsgEventsReq:
		sendEventsRep(conn, msg)
	}
}

// handleClientConnection processa requisicoes externas do cliente e confirma alertas com ACK para confiabilidade.
func handleClientConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}
	switch msg.Type {
	case MsgAlert:
		msgCopy := msg
		go enqueueAlert(msgCopy)
		ack := Message{Type: "ALERT_ACK", Status: "OK"}
		json.NewEncoder(conn).Encode(ack)
	case MsgStatusReq:
		sendStatusRep(conn)
	case MsgEventsReq:
		sendEventsRep(conn, msg)
	}
}

// handleDeviceRegistration atualiza o estado do drone no gateway e mantem o mapeamento de controle e disponibilidade.
func handleDeviceRegistration(msg Message) {
	stateMutex.Lock()
	drone, exists := drones[msg.DroneID]
	if !exists {
		drone = &DroneState{ID: msg.DroneID}
		drones[msg.DroneID] = drone
	}
	drone.ControlAddr = msg.Content
	drone.GatewayAtual = gatewayID
	drone.Status = msg.Status
	drone.MissionActive = strings.EqualFold(msg.Status, DroneBusy)
	drone.MissionInfo = msg.MissionInfo
	drone.LastHeartbeat = time.Now()
	drone.LastUpdate = time.Now()
	activeBeacons[msg.DroneID] = drone.LastHeartbeat
	stateMutex.Unlock()

	droneName := normalizeDroneID(msg.DroneID)
	logEvent(fmt.Sprintf("[DRONE] Drone %s registrado com status %s", droneName, msg.Status))
	log.Printf("[GATEWAY/%s] [DRONE] Drone %s registrado. Status: %s", gatewayID, droneName, msg.Status)
}

// handleDroneHeartbeat atualiza o estado do drone e responde com ACK, assegurando deteccao de falha continua.
func handleDroneHeartbeat(msg Message, conn net.Conn) {
	stateMutex.Lock()
	drone, exists := drones[msg.DroneID]
	if !exists {
		drone = &DroneState{ID: msg.DroneID}
		drones[msg.DroneID] = drone
	}
	if msg.Content != "" {
		drone.ControlAddr = msg.Content
	}
	drone.GatewayAtual = gatewayID
	drone.Status = msg.Status
	drone.MissionActive = strings.EqualFold(msg.Status, DroneBusy)
	drone.MissionInfo = msg.MissionInfo
	drone.LastHeartbeat = time.Now()
	drone.LastUpdate = time.Now()
	activeBeacons[msg.DroneID] = drone.LastHeartbeat
	ownsDrone := droneOwners[msg.DroneID] == gatewayID
	stateMutex.Unlock()

	droneName := normalizeDroneID(msg.DroneID)
	logEvent(fmt.Sprintf("[HEARTBEAT] Drone %s heartbeat recebido", droneName))
	log.Printf("[GATEWAY/%s] [HEARTBEAT] Drone %s heartbeat recebido", gatewayID, droneName)

	if ownsDrone && !strings.EqualFold(msg.Status, DroneBusy) {
		releaseCS(msg.DroneID, true)
	}

	ack := Message{Type: MsgHeartbeatAck, DroneID: msg.DroneID, GatewayID: gatewayID, Status: "OK"}
	if err := json.NewEncoder(conn).Encode(ack); err != nil {
		log.Printf("[GATEWAY/%s] [HEARTBEAT] Falha ao enviar ACK ao drone %s: %v", gatewayID, droneName, err)
	}
}

// sendStatusRep responde com o status local completo de drones e fila para consultas de supervisao.
func sendStatusRep(conn net.Conn) {
	stateMutex.Lock()
	payload := map[string]string{}
	payload["gateway_id"] = gatewayID
	payload["queue_size"] = fmt.Sprintf("%d", reqQueue.Len())
	for _, drone := range drones {
		keyPrefix := fmt.Sprintf("drone_%s_", drone.ID)
		payload[keyPrefix+"status"] = drone.Status
		payload[keyPrefix+"gateway_atual"] = drone.GatewayAtual
		payload[keyPrefix+"control_addr"] = drone.ControlAddr
		payload[keyPrefix+"mission_active"] = fmt.Sprintf("%t", drone.MissionActive)
		missionInfo := drone.MissionInfo
		if missionInfo == "" || strings.HasPrefix(strings.ToLower(missionInfo), "em missão até") {
			if currentReq, ok := myCurrentReq[drone.ID]; ok && currentReq.Type == MsgRequest && currentReq.Occurrence != "" {
				missionInfo = currentReq.Occurrence
			}
		}
		if drone.Status == DroneFailed {
			missionInfo = "Drone abatido"
		}
		payload[keyPrefix+"mission_info"] = missionInfo
		if currentReq, ok := myCurrentReq[drone.ID]; ok && currentReq.Type == MsgRequest {
			payload[keyPrefix+"priority"] = fmt.Sprintf("%d", currentReq.Priority)
		}
		payload[keyPrefix+"ultima_atualizacao"] = fmt.Sprintf("%d", drone.LastUpdate.Unix())
	}
	stateMutex.Unlock()
	queue := queuePreviewItems(4)
	if err := json.NewEncoder(conn).Encode(Message{Type: MsgStatusRep, Payload: payload, Queue: queue}); err != nil {
		log.Printf("[GATEWAY/%s] [STATUS] Erro ao enviar STATUS_REP: %v", gatewayID, err)
	}
}

// queuePreviewItems cria um snapshot da fila de prioridade para visualizacao sem alterar a estrutura do heap.
func queuePreviewItems(limit int) []AlertRequest {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	if reqQueue.Len() == 0 {
		return nil
	}

	temp := make(PriorityQueue, reqQueue.Len())
	for i, req := range reqQueue {
		temp[i] = req
	}
	heap.Init(&temp)

	preview := make([]AlertRequest, 0, limit)
	for i := 0; i < limit && temp.Len() > 0; i++ {
		req := heap.Pop(&temp).(*AlertRequest)
		preview = append(preview, *req)
	}
	return preview
}

// sendEventsRep retorna o log de eventos para auditoria de comportamento distribuido.
func sendEventsRep(conn net.Conn, msg Message) {
	count := 5
	if msg.Payload != nil {
		if s, ok := msg.Payload["count"]; ok {
			if v, err := strconv.Atoi(s); err == nil && v > 0 {
				count = v
			}
		}
	}
	eventMutex.Lock()
	events := append([]string(nil), eventLog...)
	eventMutex.Unlock()

	payload := map[string]string{}
	for i := 0; i < count && i < len(events); i++ {
		payload[fmt.Sprintf("event_%d", i+1)] = events[i]
	}
	json.NewEncoder(conn).Encode(Message{Type: MsgEventsRep, Payload: payload})
}

// handleRARequest decide se o REQUEST de acesso ao drone deve ser atendido
// imediatamente ou adiado para garantir exclusão mútua distribuída.
func handleRARequest(msg Message) {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	droneID := msg.DroneID
	inCS := droneOwners[droneID] == gatewayID
	wantCS := requestingCS[droneID]
	myReq := myCurrentReq[droneID]
	deferReply := false

	if inCS {
		deferReply = true
	} else if wantCS {
		if myReq.Priority < msg.Priority {
			deferReply = true
		} else if myReq.Priority == msg.Priority {
			if myReq.Lamport < msg.Lamport {
				deferReply = true
			} else if myReq.Lamport == msg.Lamport {
				if gatewayID < msg.GatewayID {
					deferReply = true
				}
			}
		}
	}

	if deferReply {
		deferred[droneID] = append(deferred[droneID], msg)
		event := fmt.Sprintf("[R-A] REQUEST adiado para drone %s de %s", droneID, msg.GatewayID)
		logEvent(event)
		log.Printf("[GATEWAY/%s] [R-A] %s", gatewayID, event)
		return
	}

	reply := Message{Type: MsgReply, DroneID: droneID, GatewayID: gatewayID, Lamport: tickLamport(0)}
	go sendDirect(msg.GatewayID, reply)
}

// handleRAReply registra respostas de peers e avanca o consenso de secao critica.
func handleRAReply(msg Message) {
	stateMutex.Lock()
	if !requestingCS[msg.DroneID] {
		stateMutex.Unlock()
		return
	}
	repliesCount[msg.DroneID]++
	stateMutex.Unlock()
	log.Printf("[GATEWAY/%s] [R-A] Reply recebido para drone %s de %s", gatewayID, msg.DroneID, msg.GatewayID)
	if ch := getReplyChannel(msg.DroneID, msg.GatewayID); ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// handleRARelease processa a liberacao de secao critica e limpa o estado associado ao drone.
func handleRARelease(msg Message) {
	stateMutex.Lock()
	if drone, ok := drones[msg.DroneID]; ok {
		drone.Status = DroneAvailable
		drone.MissionActive = false
		drone.MissionInfo = ""
		drone.GatewayAtual = msg.GatewayID
		drone.LastUpdate = time.Now()
	}
	if msg.RequestID != "" {
		removeAlertFromQueue(msg.RequestID)
	}
	droneOwners[msg.DroneID] = ""
	stateMutex.Unlock()
	notifyQueueProcessor()
	log.Printf("[GATEWAY/%s] [R-A] Drone %s liberado por %s", gatewayID, msg.DroneID, msg.GatewayID)
}

// handleAlertClaim processa a confirmacao de que outro gateway assumiu a requisicao para evitar duplicidade.
func handleAlertClaim(msg Message) {
	if msg.RequestID == "" {
		return
	}
	stateMutex.Lock()
	if claimedAlerts[msg.RequestID] {
		stateMutex.Unlock()
		return
	}
	markAlertClaimed(msg.RequestID)
	removeAlertFromQueue(msg.RequestID)
	stateMutex.Unlock()
	notifyQueueProcessor()
	log.Printf("[GATEWAY/%s] [R-A] Alerta %s assumido por %s", gatewayID, msg.RequestID, msg.GatewayID)
}

// releaseCS libera a secao critica local e reativa o processamento da fila de prioridade.
func releaseCS(droneID string, available bool) {
	stateMutex.Lock()
	deferredList := deferred[droneID]
	deferred[droneID] = nil
	currentReq := myCurrentReq[droneID]
	myCurrentReq[droneID] = Message{}
	requestingCS[droneID] = false
	repliesCount[droneID] = 0
	if available {
		droneOwners[droneID] = ""
		if drone, ok := drones[droneID]; ok {
			drone.Status = DroneAvailable
			drone.MissionActive = false
			drone.MissionInfo = ""
			drone.LastUpdate = time.Now()
		}
	}
	stateMutex.Unlock()

	if available {
		broadcastPeerMsg(Message{Type: MsgRelease, DroneID: droneID, GatewayID: gatewayID, RequestID: currentReq.RequestID, Lamport: tickLamport(0)})
		notifyQueueProcessor()
	}

	for _, pending := range deferredList {
		reply := Message{Type: MsgReply, DroneID: droneID, GatewayID: gatewayID, Lamport: tickLamport(0)}
		go sendDirect(pending.GatewayID, reply)
	}

	if !available && currentReq.Type != "" {
		stateMutex.Lock()
		heap.Push(&reqQueue, &AlertRequest{RequestID: currentReq.RequestID, Occurrence: currentReq.Occurrence, Priority: currentReq.Priority, Lamport: currentReq.Lamport, GatewayID: currentReq.GatewayID, Timestamp: currentReq.Timestamp, RetryCount: currentReqRetries[droneID]})
		persistQueueStateLocked()
		stateMutex.Unlock()
		logEvent(fmt.Sprintf("[R-A] Reenfileirando requisição do drone %s após falha", droneID))
		log.Printf("[GATEWAY/%s] [R-A] Reenfileirando requisição do drone %s após falha", gatewayID, droneID)
		notifyQueueProcessor()
	}
}

// processQueueLoop mantém a fila de alertas e tenta iniciar Ricart-Agrawala
// para drones disponíveis, ignorando drones em backoff temporário.
func processQueueLoop() {
	for {
		select {
		case <-queueNotify:
		case <-time.After(1 * time.Second):
		}
		stateMutex.Lock()
		if reqQueue.Len() == 0 {
			stateMutex.Unlock()
			continue
		}

		reqIndex := nextReadyRequestIndex()
		if reqIndex == -1 {
			stateMutex.Unlock()
			continue
		}

		var targetDrone string
		for _, drone := range drones {
			if drone.Status == DroneAvailable && drone.ControlAddr != "" && !isDroneUnavailable(drone.ID) {
				targetDrone = drone.ID
				break
			}
		}

		if targetDrone == "" {
			stateMutex.Unlock()
			continue
		}

		if !requestingCS[targetDrone] {
			req := heap.Remove(&reqQueue, reqIndex).(*AlertRequest)
			if req.SuspendedUntilUnix != 0 && time.Now().After(time.Unix(0, req.SuspendedUntilUnix)) {
				req.SuspendedUntilUnix = 0
			}
			persistQueueStateLocked()
			logEvent(fmt.Sprintf("[R-A] Iniciando R-A para drone %s com prioridade %d", targetDrone, req.Priority))
			log.Printf("[GATEWAY/%s] [R-A] Iniciando R-A para drone %s com prioridade %d", gatewayID, targetDrone, req.Priority)
			requestingCS[targetDrone] = true
			repliesCount[targetDrone] = 0
			currentReqRetries[targetDrone] = req.RetryCount
			myCurrentReq[targetDrone] = Message{Type: MsgRequest, DroneID: targetDrone, GatewayID: gatewayID, Priority: req.Priority, Lamport: req.Lamport, Occurrence: req.Occurrence, RequestID: req.RequestID}
			stateMutex.Unlock()

			msg := myCurrentReq[targetDrone]
			msg.Lamport = tickLamport(0)
			go waitForReplies(targetDrone, msg)
			continue
		}
		stateMutex.Unlock()
	}
}

// waitForReplies aguarda os REPLYs de todos os peers online e decide
// se o gateway pode entrar em seção crítica para despachar o drone.
func waitForReplies(droneID string, msg Message) {
	stateMutex.Lock()
	if !requestingCS[droneID] {
		stateMutex.Unlock()
		return
	}
	activePeers := []string{}
	for _, peerID := range peerIDs {
		if time.Now().Before(peerOfflineUntil[peerID]) {
			continue
		}
		activePeers = append(activePeers, peerID)
	}
	stateMutex.Unlock()

	log.Printf("[GATEWAY/%s] [R-A] Peers ativos para %s: %v", gatewayID, droneID, activePeers)

	if len(activePeers) == 0 {
		stateMutex.Lock()
		requestingCS[droneID] = false
		droneOwners[droneID] = gatewayID
		if drone, ok := drones[droneID]; ok {
			drone.Status = DroneBusy
			drone.MissionActive = true
			drone.MissionInfo = msg.Occurrence
		}
		stateMutex.Unlock()
		logEvent(fmt.Sprintf("[R-A] Região crítica obtida para drone %s sem peers ativos", droneID))
		log.Printf("[GATEWAY/%s] [R-A] Região crítica obtida para drone %s sem peers ativos", gatewayID, droneID)
		markAlertClaimed(msg.RequestID)
		go broadcastPeerMsg(Message{Type: MsgAlertClaim, RequestID: msg.RequestID, GatewayID: gatewayID, Lamport: tickLamport(0)})
		go dispatchDrone(droneID)
		return
	}

	replyChannelMutex.Lock()
	replyChannels[droneID] = make(map[string]chan struct{}, len(activePeers))
	replyChannelMutex.Unlock()
	defer cleanupReplyChannels(droneID)

	type attemptResult struct {
		peerID string
		err    error
	}
	results := make(chan attemptResult, len(activePeers))

	for _, peerID := range activePeers {
		channel := make(chan struct{}, 1)
		replyChannelMutex.Lock()
		replyChannels[droneID][peerID] = channel
		replyChannelMutex.Unlock()

		go func(p string) {
			results <- attemptResult{peerID: p, err: sendDirectOnce(p, msg)}
		}(peerID)
	}

	pendingPeers := make([]string, 0, len(activePeers))
	for i := 0; i < len(activePeers); i++ {
		res := <-results
		if res.err != nil {
			stateMutex.Lock()
			peerOfflineUntil[res.peerID] = time.Now().Add(10 * time.Second)
			stateMutex.Unlock()
			replyChannelMutex.Lock()
			delete(replyChannels[droneID], res.peerID)
			replyChannelMutex.Unlock()
			log.Printf("[GATEWAY/%s] [R-A] Peer %s offline ao enviar REQUEST: %v", gatewayID, res.peerID, res.err)
			continue
		}
		pendingPeers = append(pendingPeers, res.peerID)
	}
	close(results)

	if len(pendingPeers) == 0 {
		stateMutex.Lock()
		requestingCS[droneID] = false
		droneOwners[droneID] = gatewayID
		if drone, ok := drones[droneID]; ok {
			drone.Status = DroneBusy
			drone.MissionActive = true
			drone.MissionInfo = msg.Occurrence
		}
		stateMutex.Unlock()
		logEvent(fmt.Sprintf("[R-A] Região crítica obtida para drone %s sem peers alcançáveis", droneID))
		log.Printf("[GATEWAY/%s] [R-A] Região crítica obtida para drone %s sem peers alcançáveis", gatewayID, droneID)
		markAlertClaimed(msg.RequestID)
		go broadcastPeerMsg(Message{Type: MsgAlertClaim, RequestID: msg.RequestID, GatewayID: gatewayID, Lamport: tickLamport(0)})
		go dispatchDrone(droneID)
		return
	}

	gotReplies := 0
	for _, peerID := range pendingPeers {
		ch := getReplyChannel(droneID, peerID)
		if ch == nil {
			continue
		}
		select {
		case <-ch:
			gotReplies++
		case <-time.After(1 * time.Second):
			stateMutex.Lock()
			peerOfflineUntil[peerID] = time.Now().Add(10 * time.Second)
			stateMutex.Unlock()
			gotReplies++
			log.Printf("[GATEWAY/%s] [R-A] Peer sem resposta ou offline: %s", gatewayID, peerID)
		}
	}

	replyChannelMutex.Lock()
	delete(replyChannels, droneID)
	replyChannelMutex.Unlock()

	stateMutex.Lock()
	if requestingCS[droneID] && gotReplies == len(pendingPeers) {
		requestingCS[droneID] = false
		droneOwners[droneID] = gatewayID
		if drone, ok := drones[droneID]; ok {
			drone.Status = DroneBusy
			drone.MissionActive = true
			drone.MissionInfo = msg.Occurrence
		}
		markAlertClaimed(msg.RequestID)
		stateMutex.Unlock()
		go broadcastPeerMsg(Message{Type: MsgAlertClaim, RequestID: msg.RequestID, GatewayID: gatewayID, Lamport: tickLamport(0)})
		logEvent(fmt.Sprintf("[R-A] Região crítica obtida para drone %s", droneID))
		log.Printf("[GATEWAY/%s] [R-A] Região crítica obtida para drone %s", gatewayID, droneID)
		go dispatchDrone(droneID)
		return
	}
	if requestingCS[droneID] {
		requestingCS[droneID] = false
		req := myCurrentReq[droneID]
		heap.Push(&reqQueue, &AlertRequest{RequestID: req.RequestID, Occurrence: req.Occurrence, Priority: req.Priority, Lamport: req.Lamport, GatewayID: req.GatewayID, Timestamp: time.Now().Unix(), RetryCount: currentReqRetries[droneID]})
		persistQueueStateLocked()
		log.Printf("[GATEWAY/%s] [R-A] Falha ao obter respostas de quorum para drone %s, repondo fila", gatewayID, droneID)
	}
	stateMutex.Unlock()
}

// dispatchDrone tenta enviar comando DISPATCH ao drone e trata falhas de conexão.
func dispatchDrone(droneID string) {
	stateMutex.Lock()
	drone, ok := drones[droneID]
	currentReq := myCurrentReq[droneID]
	owned := droneOwners[droneID] == gatewayID
	busy := ok && drone.Status == DroneBusy
	controlAddr := ""
	if ok {
		controlAddr = drone.ControlAddr
		if drone.MissionInfo == "" && currentReq.Occurrence != "" {
			drone.MissionInfo = currentReq.Occurrence
		}
	}
	stateMutex.Unlock()
	if !ok || controlAddr == "" || !owned || !busy {
		log.Printf("[GATEWAY/%s] [FALHA] Drone %s perdeu a seção crítica antes do despacho", gatewayID, normalizeDroneID(droneID))
		handleLocalDroneFailure(droneID, "seção crítica perdida")
		return
	}

	conn, err := net.DialTimeout("tcp", controlAddr, 3*time.Second)
	if err != nil {
		log.Printf("[GATEWAY/%s] [FALHA] Não foi possível conectar ao drone %s: %v", gatewayID, normalizeDroneID(droneID), err)
		handleLocalDroneFailure(droneID, "conexão falhou")
		return
	}
	defer conn.Close()

	msg := Message{Type: "DISPATCH", DroneID: droneID, GatewayID: gatewayID, Lamport: tickLamport(0), Occurrence: currentReq.Occurrence}
	if err := json.NewEncoder(conn).Encode(&msg); err != nil {
		log.Printf("[GATEWAY/%s] [FALHA] Erro ao enviar DISPATCH ao drone %s: %v", gatewayID, normalizeDroneID(droneID), err)
		handleLocalDroneFailure(droneID, "envio DISPATCH falhou")
		return
	}

	log.Printf("[GATEWAY/%s] [DESPACHO] Drone %s despachado com sucesso", gatewayID, normalizeDroneID(droneID))
}

// handleDroneFailed marca drones como falhos e aciona replanejamento da fila em toda a malha.
func handleDroneFailed(msg Message) {
	stateMutex.Lock()
	drone, ok := drones[msg.DroneID]
	if ok {
		drone.Status = DroneFailed
		drone.MissionActive = false
		drone.MissionInfo = "falha detectada"
		drone.LastUpdate = time.Now()
	}
	wasOwner := droneOwners[msg.DroneID] == gatewayID
	droneOwners[msg.DroneID] = ""
	stateMutex.Unlock()

	droneName := normalizeDroneID(msg.DroneID)
	logEvent(fmt.Sprintf("[FALHA] Drone %s marcado como FALHO", droneName))
	log.Printf("[GATEWAY/%s] [FALHA] Drone %s marcado como FALHO por broadcast", gatewayID, droneName)
	if wasOwner {
		releaseCS(msg.DroneID, false)
	}
}

// handleLocalDroneFailure reage à falha de um drone local.
// Se o drone estiver em missão, a requisição é reenfileirada com backoff exponencial.
func handleLocalDroneFailure(droneID, reason string) {
	stateMutex.Lock()
	drone, ok := drones[droneID]
	if ok {
		drone.Status = DroneFailed
		drone.MissionActive = false
		if drone.MissionInfo == "" || reason != "heartbeat ausente" {
			drone.MissionInfo = reason
		}
		drone.GatewayAtual = ""
		drone.LastUpdate = time.Now()
	}
	currentReq := myCurrentReq[droneID]
	downOwner := droneOwners[droneID] == gatewayID
	droneOwners[droneID] = ""
	requestingCS[droneID] = false
	repliesCount[droneID] = 0
	myCurrentReq[droneID] = Message{}
	stateMutex.Unlock()

	logEvent(fmt.Sprintf("[FALHA] Drone %s falhou localmente: %s", normalizeDroneID(droneID), reason))
	log.Printf("[GATEWAY/%s] [FALHA] Drone %s falhou localmente: %s", gatewayID, normalizeDroneID(droneID), reason)
	broadcastPeerMsg(Message{Type: MsgDroneFailed, DroneID: droneID, GatewayID: gatewayID, Lamport: tickLamport(0)})

	if downOwner && currentReq.Type != "" {
		retryCount := currentReqRetries[droneID] + 1
		delay := 10 * time.Second
		if retryCount > 0 {
			delay = delay * time.Duration(1<<uint(retryCount-1))
		}
		if delay > 2*time.Minute {
			delay = 2 * time.Minute
		}

		req := &AlertRequest{
			RequestID:          currentReq.RequestID,
			Occurrence:         currentReq.Occurrence,
			Priority:           currentReq.Priority,
			Lamport:            currentReq.Lamport,
			GatewayID:          currentReq.GatewayID,
			Timestamp:          currentReq.Timestamp,
			RetryCount:         retryCount,
			SuspendedUntilUnix: time.Now().Add(delay).Unix(),
		}

		stateMutex.Lock()
		droneUnavailableUntil[droneID] = time.Now().Add(delay)
		heap.Push(&reqQueue, req)
		persistQueueStateLocked()
		delete(currentReqRetries, droneID)
		stateMutex.Unlock()
		logEvent(fmt.Sprintf("[R-A] Reenfileirando requisição do drone %s após falha com backoff %s", droneID, delay))
		log.Printf("[GATEWAY/%s] [R-A] Reenfileirando requisição do drone %s após falha com backoff %s", gatewayID, droneID, delay)
		notifyQueueProcessor()
	}
}

// syncStateOnStart sincroniza o estado do gateway com peers ao iniciar para garantir consistencia inicial.
func syncStateOnStart() {
	time.Sleep(2 * time.Second)
	msg := Message{Type: MsgSnapshotRequest, GatewayID: gatewayID, Lamport: tickLamport(0)}

	for _, peerID := range peerIDs {
		stateMutex.Lock()
		if time.Now().Before(peerOfflineUntil[peerID]) {
			stateMutex.Unlock()
			continue
		}
		addr, ok := peerAddrsByID[peerID]
		stateMutex.Unlock()
		if !ok {
			continue
		}

		log.Printf("[GATEWAY/%s] [SYNC] Solicitando estado de %s (%s)", gatewayID, peerID, addr)
		dialer := &net.Dialer{}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		cancel()
		if err != nil {
			markPeerOffline(peerID, 10*time.Second, fmt.Sprintf("falha de conexão: %v", err))
			log.Printf("[GATEWAY/%s] [SYNC] Peer %s offline ao conectar: %v", gatewayID, peerID, err)
			continue
		}

		conn.SetDeadline(time.Now().Add(1 * time.Second))
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			conn.Close()
			markPeerOffline(peerID, 10*time.Second, fmt.Sprintf("erro ao enviar snapshot request: %v", err))
			log.Printf("[GATEWAY/%s] [SYNC] Falha ao enviar snapshot request para %s: %v", gatewayID, peerID, err)
			continue
		}

		var reply Message
		if err := json.NewDecoder(conn).Decode(&reply); err != nil {
			conn.Close()
			markPeerOffline(peerID, 10*time.Second, fmt.Sprintf("sem resposta ao snapshot: %v", err))
			log.Printf("[GATEWAY/%s] [SYNC] Peer %s não respondeu snapshot: %v", gatewayID, peerID, err)
			continue
		}
		conn.Close()

		if reply.Type != MsgStateSync {
			log.Printf("[GATEWAY/%s] [SYNC] Peer %s respondeu com tipo inesperado: %s", gatewayID, peerID, reply.Type)
			continue
		}

		stateMutex.Lock()
		reqQueue = make(PriorityQueue, 0)
		seenAlerts = make(map[string]bool)
		stateMutex.Unlock()

		receiveStateSync(reply)
		persistQueueState()
		log.Printf("[GATEWAY/%s] [SYNC] Estado sincronizado com peer %s", gatewayID, peerID)
		return
	}

	log.Printf("[GATEWAY/%s] [SYNC] Nenhum peer disponível para sincronizar. Inicializando sem fila replicada.", gatewayID)
}

// sendStateSync envia um snapshot local de estado a um peer solicitante.
func sendStateSync(conn net.Conn) {
	stateMutex.Lock()
	payload := make(map[string]string)
	for _, drone := range drones {
		base := fmt.Sprintf("drone_%s_", drone.ID)
		payload[base+"status"] = drone.Status
		payload[base+"gateway_atual"] = drone.GatewayAtual
		payload[base+"control_addr"] = drone.ControlAddr
		payload[base+"mission_active"] = fmt.Sprintf("%t", drone.MissionActive)
		payload[base+"mission_info"] = drone.MissionInfo
		payload[base+"ultimo_heartbeat"] = fmt.Sprintf("%d", drone.LastHeartbeat.UnixNano())
		payload[base+"ultima_atualizacao"] = fmt.Sprintf("%d", drone.LastUpdate.UnixNano())
		payload[base+"setor_base"] = drone.SetorBase
	}
	queueCopy := make([]AlertRequest, 0, reqQueue.Len())
	for _, req := range reqQueue {
		queueCopy = append(queueCopy, *req)
	}
	stateMutex.Unlock()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(conn).Encode(Message{Type: MsgStateSync, GatewayID: gatewayID, Lamport: tickLamport(0), Payload: payload, Queue: queueCopy}); err != nil {
		log.Printf("[GATEWAY/%s] [SYNC] Falha ao enviar STATE_SYNC: %v", gatewayID, err)
	}
}

// receiveStateSync assimila o snapshot de peers para convergencia de estado distribuido.
func receiveStateSync(msg Message) {
	stateMutex.Lock()
	for key, value := range msg.Payload {
		if !strings.HasPrefix(key, "drone_") {
			continue
		}
		remainder := strings.TrimPrefix(key, "drone_")
		last := strings.LastIndex(remainder, "_")
		if last <= 0 {
			continue
		}
		droneID := remainder[:last]
		field := remainder[last+1:]
		drone, ok := drones[droneID]
		if !ok {
			drone = &DroneState{ID: droneID}
			drones[droneID] = drone
		}
		update := time.Now()
		switch field {
		case "status":
			drone.Status = value
		case "gateway_atual":
			drone.GatewayAtual = value
		case "control_addr":
			drone.ControlAddr = value
		case "mission_active":
			drone.MissionActive = value == "true"
		case "mission_info":
			drone.MissionInfo = value
		case "ultimo_heartbeat":
			if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
				drone.LastHeartbeat = time.Unix(0, ts)
			}
		case "ultima_atualizacao":
			if ts, err := strconv.ParseInt(value, 10, 64); err == nil {
				update = time.Unix(0, ts)
			}
			drone.LastUpdate = update
		case "setor_base":
			drone.SetorBase = value
		}
	}
	stateMutex.Unlock()
	mergeQueueFromStateSync(msg.Queue)
	log.Printf("[GATEWAY/%s] [SYNC] Estado sincronizado recebido de %s", gatewayID, msg.GatewayID)
}

// monitorLocalDroneHeartbeats detecta drones sem heartbeat e aciona recuperacao imediata.
func monitorLocalDroneHeartbeats() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		stateMutex.Lock()
		localCopy := make(map[string]struct {
			last    time.Time
			gateway string
			status  string
		})
		for id, drone := range drones {
			localCopy[id] = struct {
				last    time.Time
				gateway string
				status  string
			}{last: drone.LastHeartbeat, gateway: drone.GatewayAtual, status: drone.Status}
		}
		stateMutex.Unlock()

		for droneID, info := range localCopy {
			if time.Since(info.last) > 15*time.Second {
				if info.gateway == gatewayID && info.status != DroneFailed {
					handleLocalDroneFailure(droneID, "heartbeat ausente")
				}
			}
		}
	}
}

// markPeerOffline marca peers como indisponiveis ao detectar falha de comunicacao entre gateways.
func markPeerOffline(peerID string, duration time.Duration, reason string) {
	stateMutex.Lock()
	peerOfflineUntil[peerID] = time.Now().Add(duration)
	stateMutex.Unlock()
	log.Printf("[GATEWAY/%s] [PEER] Marcando peer %s offline por %s: %s", gatewayID, peerID, duration, reason)
}

// broadcastPeerMsg propaga mensagens criticas a todos os peers online com retry e disciplina de falha.
func broadcastPeerMsg(msg Message) {
	for _, peerID := range peerIDs {
		if time.Now().Before(peerOfflineUntil[peerID]) {
			continue
		}
		go func(pID string) {
			if err := sendDirect(pID, msg); err != nil {
				log.Printf("[GATEWAY/%s] [PEER] Falha broadcast para %s: %v", gatewayID, pID, err)
			}
		}(peerID)
	}
}

// sendDirect envia mensagens diretas a um peer usando retry e confirmacao de chegada.
func sendDirect(targetGateway string, msg Message) error {
	return sendDirectWithRetry(targetGateway, msg, 3)
}

func sendDirectOnce(targetGateway string, msg Message) error {
	addr, ok := peerAddrsByID[targetGateway]
	if !ok {
		return fmt.Errorf("endereço do peer %s desconhecido", targetGateway)
	}

	dialer := &net.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(1 * time.Second))
	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return err
	}
	if shouldPeerAck(msg.Type) {
		var ack Message
		if err := json.NewDecoder(conn).Decode(&ack); err != nil {
			return err
		}
		if ack.Type != MsgAck || ack.Status != "OK" {
			return fmt.Errorf("ack inválido do peer %s: %s", targetGateway, ack.Type)
		}
	}
	markPeerOnline(targetGateway)
	return nil
}

// sendDirectWithRetry retransmite mensagens P2P ate confirmacao ou erro definitivo.
func sendDirectWithRetry(targetGateway string, msg Message, maxAttempts int) error {
	addr, ok := peerAddrsByID[targetGateway]
	if !ok {
		return fmt.Errorf("endereço do peer %s desconhecido", targetGateway)
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			continue
		}
		conn.SetDeadline(time.Now().Add(4 * time.Second))
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			conn.Close()
			lastErr = err
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			continue
		}
		if shouldPeerAck(msg.Type) {
			var ack Message
			if err := json.NewDecoder(conn).Decode(&ack); err != nil {
				conn.Close()
				lastErr = err
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				continue
			}
			if ack.Type != MsgAck || ack.Status != "OK" {
				conn.Close()
				lastErr = fmt.Errorf("ack inválido do peer %s: %s", targetGateway, ack.Type)
				time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				continue
			}
		}
		conn.Close()
		markPeerOnline(targetGateway)
		return nil
	}

	markPeerOffline(targetGateway, 15*time.Second, fmt.Sprintf("nenhuma tentativa bem-sucedida após %d tentativas: %v", maxAttempts, lastErr))
	return lastErr
}

// shouldPeerAck determina quais mensagens do protocolo peer exigem acknowledgement seguro.
func shouldPeerAck(msgType string) bool {
	switch msgType {
	case MsgRequest, MsgReply, MsgRelease, MsgAlert, MsgDroneFailed, MsgDeviceReg, MsgPeerHeartbeat:
		return true
	default:
		return false
	}
}

// sendPeerAck envia confirmacao de recebimento para mensagens criticas de coordenacao.
func sendPeerAck(conn net.Conn, msg Message) {
	ack := Message{Type: MsgAck, Status: "OK", RequestID: msg.RequestID, GatewayID: gatewayID, Lamport: tickLamport(msg.Lamport)}
	json.NewEncoder(conn).Encode(ack)
}

// markPeerOnline reintegra peers recuperados a malha distribuida.
func markPeerOnline(peerID string) {
	stateMutex.Lock()
	delete(peerOfflineUntil, peerID)
	peerFailureCount[peerID] = 0
	stateMutex.Unlock()
	log.Printf("[GATEWAY/%s] [PEER] Peer %s online", gatewayID, peerID)
}

// startPeerHealthMonitor monitora saude dos peers e preserva a malha contra falhas de gateway.
func startPeerHealthMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, peerID := range peerIDs {
			stateMutex.Lock()
			offlineUntil, peerOffline := peerOfflineUntil[peerID]
			stateMutex.Unlock()
			if peerOffline && time.Now().Before(offlineUntil) {
				continue
			}
			peerMsg := Message{Type: MsgPeerHeartbeat, GatewayID: gatewayID, Timestamp: time.Now().UnixNano()}
			go func(pID string) {
				if err := sendDirect(pID, peerMsg); err != nil {
					shouldOffline := false
					failureCount := 0
					stateMutex.Lock()
					peerFailureCount[pID]++
					failureCount = peerFailureCount[pID]
					if failureCount >= 3 {
						shouldOffline = true
						peerFailureCount[pID] = 0
					}
					stateMutex.Unlock()
					if shouldOffline {
						markPeerOffline(pID, 15*time.Second, fmt.Sprintf("heartbeat falhou %d vezes: %v", failureCount, err))
					}
					return
				}
				stateMutex.Lock()
				peerFailureCount[pID] = 0
				stateMutex.Unlock()
			}(peerID)
		}
	}
}

// getReplyChannel retorna o canal de reply para consenso de peers sobre acesso ao drone.
func getReplyChannel(droneID, peerID string) chan struct{} {
	replyChannelMutex.Lock()
	defer replyChannelMutex.Unlock()
	if peerMap, ok := replyChannels[droneID]; ok {
		return peerMap[peerID]
	}
	return nil
}

// cleanupReplyChannels remove canais de reply antigos do mapa para evitar vazamentos.
func cleanupReplyChannels(droneID string) {
	replyChannelMutex.Lock()
	delete(replyChannels, droneID)
	replyChannelMutex.Unlock()
}

// tickLamport atualiza o relogio logico Lamport de forma thread-safe para ordenacao de eventos.
func tickLamport(recv int) int {
	lamportMutex.Lock()
	defer lamportMutex.Unlock()
	if recv > lamportClock {
		lamportClock = recv
	}
	lamportClock++
	return lamportClock
}

// updateLamport ajusta o relogio Lamport ao receber mensagens de outros gateways.
func updateLamport(recv int) {
	lamportMutex.Lock()
	defer lamportMutex.Unlock()
	if recv > lamportClock {
		lamportClock = recv
	}
	lamportClock++
}

// logEvent registra eventos internos para auditoria de comportamento distribuido.
func logEvent(event string) {
	eventMutex.Lock()
	defer eventMutex.Unlock()
	if len(eventLog) >= 100 {
		eventLog = eventLog[1:]
	}
	eventLog = append(eventLog, fmt.Sprintf("%s %s", time.Now().Format(time.RFC3339), event))
}

/******************************************************************************************

Autor: Walace de Jesus Venas
Componente Curricular: TEC502 MI- CONCORRÊNCIA E CONECTIVIDADE
Concluído em: 14/05/2026
Declaro que este código foi elaborado por mim de forma individual e não contêm nenhum
trecho de código de outro colega ou de outro autor, tais como provindos de livros e
apostilas, e páginas ou documentos eletrônicos da Internet. Qualquer trecho de código
de outra autoria que não a minha está destacado com uma citação para o autor e a fonte
do código, e estou ciente que estes trechos não serão considerados para fins de avaliação.
Implementação baseada no algoritmo distribuído de exclusão mútua de Ricart-Agrawala.

*******************************************************************************************/
