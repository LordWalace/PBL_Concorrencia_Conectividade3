package main

import (
	"bufio"
	"context"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
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
	CompanyID   string            `json:"company_id,omitempty"`
	MissionID   string            `json:"mission_id,omitempty"`
	Queue       []AlertRequest    `json:"queue,omitempty"`
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
	CompanyID          string `json:"company_id,omitempty"`
	MissionID          string `json:"mission_id,omitempty"`
	AwaitingCredits    bool   `json:"awaiting_credits,omitempty"`
}

type DroneInfo struct {
	Status       string
	MissionState string
	MissionInfo  string
	Priority     int
	Gateway      string
}

type OccurrenceOption struct {
	Priority    int
	Description string
}

var occurrenceOptions = []OccurrenceOption{
	{Priority: 1, Description: "Embarcação civil à deriva"},
	{Priority: 1, Description: "Objeto não identificado"},
	{Priority: 2, Description: "Suspeita de bloqueio parcial de rota"},
	{Priority: 2, Description: "Falha de sinalização"},
	{Priority: 3, Description: "Congestionamento em corredor marítimo"},
	{Priority: 3, Description: "Inspeção visual urgente"},
	{Priority: 4, Description: "Replanejamento de tráfego por risco ambiental"},
}

// Navios genéricos usados silenciosamente para movimentar a economia do gateway
var defaultCompanies = []string{"navio-norte", "navio-sul", "navio-leste", "navio-oeste"}

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Variável de ambiente %s não configurada", key)
	}
	return val
}

func getAvailableGatewayConn(gateways map[string]string) (net.Conn, string, error) {
	for name, addr := range gateways {
		conn, err := dialTransport(addr, 3*time.Second)
		if err == nil {
			return conn, name, nil
		}
	}
	return nil, "", fmt.Errorf("nenhum gateway disponível no momento")
}

func main() {
	// Inicialização da seed para atribuição aleatória silenciosa
	rand.Seed(time.Now().UnixNano())

	clientPort := mustEnv("GATEWAY_TCP_CLIENT_PORT")
	regPort := mustEnv("GATEWAY_TCP_REG_PORT")
	gateways := map[string]string{
		"Norte": fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), clientPort),
		"Sul":   fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), clientPort),
		"Leste": fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), clientPort),
		"Oeste": fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), clientPort),
	}
	gatewaysReg := map[string]string{
		"Norte": fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), regPort),
		"Sul":   fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), regPort),
		"Leste": fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), regPort),
		"Oeste": fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), regPort),
	}

	reader := bufio.NewReader(os.Stdin)
	sectors := []string{"Norte", "Sul", "Leste", "Oeste"}

	clearScreen()
	skipNextClear := false

	for {
		if !skipNextClear {
			clearScreen()
		} else {
			skipNextClear = false
		}

		fmt.Println("======================================")
		fmt.Println("  DESBLOQUEIO DO ESTREITO HORMUZ")
		fmt.Println("======================================")
		fmt.Println("\nMenu:")
		fmt.Println("1 - Injetar Evento Ambiental (Mock de Sensor/Beacon)")
		fmt.Println("2 - Acionar Missão de Resgate (Requer Tokens / Cliente)")
		fmt.Println("3 - Ver Status do Estreito")
		fmt.Println("4 - Ver Log de Eventos")
		fmt.Println("0 - Sair")
		fmt.Print("Escolha uma opção (ou Enter para atualizar): ")

		choice := readChoice(reader)

		switch choice {
		case "1":
			clearScreen()
			sendManualAlert(reader, sectors, gatewaysReg, true)
			skipNextClear = true

		case "2":
			clearScreen()
			solveProblemMenu(reader, gateways)
			skipNextClear = true

		case "3":
			clearScreen()
			printStatus(sectors, gateways)
			fmt.Println()
			skipNextClear = true

		case "4":
			clearScreen()
			viewEventLog(reader, sectors, gateways)
			fmt.Println()
			skipNextClear = true

		case "":
			clearMenuLines(14)
			continue

		case "0":
			clearScreen()
			fmt.Println("Encerrando cliente.")
			return

		default:
			clearMenuLines(13)
			continue
		}
	}
}

func sendManualAlert(reader *bufio.Reader, sectors []string, gateways map[string]string, isEnvironmental bool) {
	if isEnvironmental {
		fmt.Println("--- INJETAR ALERTA AMBIENTAL (MOCK DE SENSOR) ---")
	} else {
		fmt.Println("--- ACIONAR MISSÃO (CLIENTE/PAGADOR) ---")
	}

	// Setor
	for i, setor := range sectors {
		fmt.Printf("%d - %s\n", i+1, setor)
	}
	fmt.Print("Selecione o setor (1-4): ")
	sectorIndex := readNumber(reader, 1, len(sectors)) - 1
	setorEscolhido := sectors[sectorIndex]

	// Ocorrência
	fmt.Println("\nOcorrências disponíveis:")
	for i, option := range occurrenceOptions {
		fmt.Printf("%d - %s (Prioridade %d)\n", i+1, option.Description, option.Priority)
	}
	fmt.Print("Escolha a ocorrência (1-7): ")
	occurrenceIndex := readNumber(reader, 1, len(occurrenceOptions)) - 1
	option := occurrenceOptions[occurrenceIndex]

	requestID := fmt.Sprintf("client:%s:%d", setorEscolhido, time.Now().UnixNano())

	var msg Message
	msg = Message{
		Type:       "ALERT",
		RequestID:  requestID,
		Priority:   option.Priority,
		Occurrence: option.Description,
		CompanyID:  "", // Vazio = Evento Ambiental
	}
	fmt.Printf("\n[BEACON MOCK] Injetando evento no setor %s...\n", setorEscolhido)
	sendWithFallback(msg, setorEscolhido, sectors, gateways)
}

func solveProblemMenu(reader *bufio.Reader, gateways map[string]string) {
	offset := 0
	limit := 5

	for {
		clearScreen()
		fmt.Println("--- RESOLVER PROBLEMA EXISTENTE ---")

		conn, _, err := getAvailableGatewayConn(gateways)
		if err != nil {
			fmt.Println("Nenhum gateway disponível para consultar problemas.")
			fmt.Println("\nPressione Enter para voltar...")
			reader.ReadString('\n')
			return
		}

		req := Message{
			Type: "PROBLEMS_REQ",
			Payload: map[string]string{
				"offset": strconv.Itoa(offset),
				"limit":  strconv.Itoa(limit),
			},
		}
		json.NewEncoder(conn).Encode(req)

		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		var rep Message
		err = json.NewDecoder(conn).Decode(&rep)
		conn.Close()

		if err != nil || rep.Type != "PROBLEMS_REP" {
			fmt.Println("Erro ao buscar problemas no gateway.")
			fmt.Println("\nPressione Enter para voltar...")
			reader.ReadString('\n')
			return
		}

		total, _ := strconv.Atoi(rep.Payload["total"])

		if total == 0 {
			fmt.Println("Nenhum problema pendente no Estreito.")
			fmt.Println("\nPressione Enter para voltar...")
			reader.ReadString('\n')
			return
		}

		fmt.Printf("Problemas pendentes (%d encontrados, exibindo paginacao):\n\n", total)
		for i, p := range rep.Queue {
			fmt.Printf("%d - %s (Prioridade: %d, Origem: %s)\n", i+1, p.Occurrence, p.Priority, p.GatewayID)
		}

		hasNext := offset+len(rep.Queue) < total
		hasPrev := offset > 0

		fmt.Println()
		if hasNext {
			fmt.Println("6 - Próximos 5")
		}
		if hasPrev {
			fmt.Println("7 - Anteriores 5")
		}
		fmt.Println("8 - Atualizar lista")
		fmt.Println("0 - Cancelar")

		fmt.Print("\nEscolha uma opção: ")
		choice := readChoice(reader)

		switch choice {
		case "0":
			return
		case "6":
			if hasNext {
				offset += limit
			}
		case "7":
			if hasPrev {
				offset -= limit
			}
		case "8":
			// Apenas repete o loop sem alterar o offset
		case "1", "2", "3", "4", "5":
			idx, err := strconv.Atoi(choice)
			if err == nil && idx > 0 && idx <= len(rep.Queue) {
				selected := rep.Queue[idx-1]
				dispatchClientMission(reader, selected, gateways)
				return
			}
		default:
			continue
		}
	}
}

func dispatchClientMission(reader *bufio.Reader, selected AlertRequest, gateways map[string]string) {
	fmt.Printf("\nVocê selecionou o problema: %s\n", selected.Occurrence)

	companyID := defaultCompanies[rand.Intn(len(defaultCompanies))]
	msg := Message{
		Type:       "MISSION_SUBMIT",
		RequestID:  selected.RequestID,
		Priority:   selected.Priority,
		Occurrence: selected.Occurrence,
		CompanyID:  companyID,
	}
	custo := selected.Priority * 10
	fmt.Printf("[CLIENTE] Transmitindo solicitacao de missao a malha...\n")
	fmt.Printf("[ECONOMIA] O navio '%s' pagará %d tokens por este despacho!\n", companyID, custo)

	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("Erro ao conectar à malha.")
		return
	}
	defer conn.Close()
	json.NewEncoder(conn).Encode(msg)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ack Message
	json.NewDecoder(conn).Decode(&ack)

	if ack.Status == "OK" {
		fmt.Printf("[CLIENTE] Missão autorizada! (ID: %s)\n", ack.MissionID)
	} else {
		fmt.Printf("[CLIENTE] Fatura retida. Motivo: %s\n", ack.Content)
	}
	fmt.Println("\nPressione Enter para continuar...")
	reader.ReadString('\n')
}

func printStatus(sectors []string, gateways map[string]string) {
	fmt.Println("--- STATUS DO ESTREITO ---")

	var wg sync.WaitGroup
	var mu sync.Mutex
	globalDrones := make(map[string]DroneInfo)
	sectorResults := make([]string, len(sectors))

	for i, sector := range sectors {
		wg.Add(1)
		go func(idx int, setor string) {
			defer wg.Done()
			addr := gateways[setor]
			conn, err := dialTransport(addr, 5*time.Second)
			if err != nil {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] OFFLINE", setor)
				return
			}
			defer conn.Close()

			if err := json.NewEncoder(conn).Encode(Message{Type: "STATUS_REQ"}); err != nil {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] Erro ao solicitar status: %v", setor, err)
				return
			}

			var reply Message
			conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			if err := json.NewDecoder(conn).Decode(&reply); err != nil || reply.Type != "STATUS_REP" {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] OFFLINE", setor)
				return
			}
			queueSize := reply.Payload["queue_size"]
			sectorResults[idx] = fmt.Sprintf("[Setor %s] ONLINE | Fila: %s", setor, queueSize)
			if len(reply.Queue) > 0 {
				sectorResults[idx] += " | Próximas:"
				for j, item := range reply.Queue {
					sectorResults[idx] += fmt.Sprintf("\n      %d) %s | P%d | Origem: %s", j+1, item.Occurrence, item.Priority, item.GatewayID)
				}
			}

			droneStates := make(map[string]DroneInfo)
			for key, value := range reply.Payload {
				if key == "queue_size" {
					continue
				}
				if !strings.HasPrefix(key, "drone_") {
					continue
				}

				trimmed := strings.TrimPrefix(key, "drone_")
				var droneID, field string
				switch {
				case strings.HasSuffix(trimmed, "_status"):
					droneID = strings.TrimSuffix(trimmed, "_status")
					field = "status"
				case strings.HasSuffix(trimmed, "_mission_active"):
					droneID = strings.TrimSuffix(trimmed, "_mission_active")
					field = "mission_active"
				case strings.HasSuffix(trimmed, "_gateway_atual"):
					droneID = strings.TrimSuffix(trimmed, "_gateway_atual")
					field = "gateway_atual"
				case strings.HasSuffix(trimmed, "_mission_info"):
					droneID = strings.TrimSuffix(trimmed, "_mission_info")
					field = "mission_info"
				case strings.HasSuffix(trimmed, "_priority"):
					droneID = strings.TrimSuffix(trimmed, "_priority")
					field = "priority"
				default:
					continue
				}

				droneID = cleanDroneName(droneID)
				info := droneStates[droneID]
				switch field {
				case "status":
					info.Status = value
				case "mission_active":
					if value == "true" {
						info.MissionState = "Em missão"
					} else {
						info.MissionState = "Disponível"
					}
				case "gateway_atual":
					info.Gateway = value
				case "mission_info":
					info.MissionInfo = value
				case "priority":
					if p, err := strconv.Atoi(value); err == nil {
						info.Priority = p
					}
				}
				droneStates[droneID] = info
			}

			mu.Lock()
			for id, info := range droneStates {
				name := cleanDroneName(id)
				existing := globalDrones[name]
				if info.Status != "" {
					existing.Status = info.Status
				}
				if info.MissionState != "" {
					existing.MissionState = info.MissionState
				}
				if info.MissionInfo != "" {
					existing.MissionInfo = info.MissionInfo
				}
				if info.Priority > 0 {
					existing.Priority = info.Priority
				}
				if info.Gateway != "" {
					existing.Gateway = info.Gateway
				}
				globalDrones[name] = existing
			}
			mu.Unlock()
		}(i, sector)
	}

	wg.Wait()

	for _, result := range sectorResults {
		fmt.Println(result)
	}

	fmt.Println("\n--- STATUS GLOBAL DA FROTA ---")
	if len(globalDrones) == 0 {
		fmt.Println("Nenhum drone conhecido no momento.")
	} else {
		keys := make([]string, 0, len(globalDrones))
		for droneID := range globalDrones {
			keys = append(keys, droneID)
		}
		sort.Strings(keys)

		for _, droneID := range keys {
			info := globalDrones[droneID]
			if info.Status == "" && info.MissionState == "" && info.Gateway == "" {
				continue
			}
			if strings.Contains(droneID, "-control") || strings.Contains(droneID, "-gateway") || strings.Contains(droneID, "-mission") || strings.Contains(droneID, "-setor") || strings.Contains(droneID, "-ultima") {
				continue
			}
			status := info.Status
			if status == "" {
				status = "DESCONHECIDO"
			}
			mission := info.MissionInfo
			if mission == "" {
				mission = info.MissionState
			}
			if mission == "" {
				mission = "Indefinido"
			}
			priorityLabel := "-"
			if info.Priority > 0 {
				priorityLabel = fmt.Sprintf("P%d", info.Priority)
			}
			gateway := info.Gateway
			if gateway == "" {
				gateway = "-"
			}
			mission = capitalizeFirst(mission)
			fmt.Printf("[%s] - Status: %s | Missão: %s | Prioridade: %s | Gateway: %s\n", formatDroneName(droneID), status, mission, priorityLabel, gateway)
		}
	}

	fmt.Println("\nPressione Enter para voltar ao menu...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func viewEventLog(reader *bufio.Reader, sectors []string, gateways map[string]string) {
	fmt.Println("--- LOG DE EVENTOS ---")
	fmt.Print("Quantos eventos deseja ver por setor? ")
	eventCount := readNumber(reader, 1, 20)
	fmt.Println()

	for _, sector := range sectors {
		addr := gateways[sector]
		conn, err := dialTransport(addr, 5*time.Second)
		if err != nil {
			fmt.Printf("[Setor %s] OFFLINE\n", sector)
			continue
		}

		request := Message{Type: "EVENTS_REQ", Payload: map[string]string{"count": strconv.Itoa(eventCount)}}
		if err := json.NewEncoder(conn).Encode(request); err != nil {
			fmt.Printf("[Setor %s] Falha ao solicitar eventos: %v\n", sector, err)
			conn.Close()
			continue
		}

		var reply Message
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err := json.NewDecoder(conn).Decode(&reply); err != nil {
			fmt.Printf("[Setor %s] Falha ao receber eventos: %v\n", sector, err)
			conn.Close()
			continue
		}
		conn.Close()

		if reply.Type != "EVENTS_REP" {
			fmt.Printf("[Setor %s] API de eventos não disponível\n", sector)
			continue
		}

		fmt.Printf("[Setor %s] Eventos recebidos:\n", sector)
		eventIndex := 1
		for eventIndex <= eventCount {
			key := fmt.Sprintf("event_%d", eventIndex)
			if value, ok := reply.Payload[key]; ok {
				fmt.Printf("   %d - %s\n", eventIndex, value)
			} else {
				break
			}
			eventIndex++
		}
		if eventIndex == 1 {
			fmt.Println("   Nenhum evento disponível.")
		}
	}

	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

func readChoice(reader *bufio.Reader) string {
	line, err := reader.ReadString('\n')
	if err != nil {
		time.Sleep(2 * time.Second)
		os.Exit(1)
		return ""
	}
	return strings.TrimSpace(line)
}

func readNumber(reader *bufio.Reader, min, max int) int {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(2 * time.Second)
			os.Exit(1)
			return 0
		}
		value, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || value < min || value > max {
			fmt.Printf("Entrada inválida. Digite um número entre %d e %d: ", min, max)
			continue
		}
		return value
	}
}

func sendWithFallback(msg Message, initialSector string, sectors []string, gateways map[string]string) {
	order := make([]string, 0, len(sectors))
	order = append(order, initialSector)
	for _, sector := range sectors {
		if sector != initialSector {
			order = append(order, sector)
		}
	}

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		for _, sector := range order {
			target := gateways[sector]
			conn, err := dialTransport(target, 3*time.Second)
			if err != nil {
				continue
			}
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				conn.Close()
				continue
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			var ack Message
			if err := json.NewDecoder(conn).Decode(&ack); err != nil {
				conn.Close()
				continue
			}
			conn.Close()

			if ack.Type == "MISSION_ACK" || ack.Type == "ALERT_ACK" {
				if ack.Status == "OK" {
					fmt.Printf("[CLIENTE] Missão autorizada no Setor %s! (ID: %s)\n", sector, ack.MissionID)
				} else {
					fmt.Printf("[CLIENTE] Fatura retida no Setor %s (Aguardando Saldo do Consórcio).\nMotivo: %s\n", sector, ack.Content)
				}
				fmt.Println("\nPressione Enter para continuar...")
				bufio.NewReader(os.Stdin).ReadString('\n')
				return
			}
		}

		if attempt < maxAttempts {
			fmt.Printf("[CLIENTE] Malha inoperante. Tentando novamente em 3 segundos... (%d/%d)\n", attempt, maxAttempts)
			time.Sleep(3 * time.Second)
		}
	}
	fmt.Println("[CLIENTE] Falha ao conectar com a malha após múltiplas tentativas.")
	fmt.Println("Pressione Enter para voltar ao menu...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func cleanDroneName(droneID string) string {
	droneID = strings.TrimPrefix(droneID, "drone_")
	droneID = strings.TrimPrefix(droneID, "Drone_")
	return strings.ReplaceAll(droneID, "_", "-")
}

func capitalizeFirst(text string) string {
	if len(text) == 0 {
		return text
	}
	return strings.ToUpper(string(text[0])) + text[1:]
}

func formatDroneName(droneID string) string {
	id := strings.ToLower(droneID)
	switch {
	case strings.Contains(id, "norte"):
		return "Drone-N"
	case strings.Contains(id, "sul"):
		return "Drone-S"
	case strings.Contains(id, "leste"):
		return "Drone-L"
	case strings.Contains(id, "oeste"):
		return "Drone-O"
	default:
		return cleanDroneName(droneID)
	}
}

func clearScreen() {
	fmt.Print("\033[H\033[2J\033[3J")
}

func clearMenuLines(linhas int) {
	fmt.Printf("\033[%dA\033[J", linhas)
}

// --- QUIC TRANSPORT ABSTRACTION ---

var (
	quicConns = make(map[string]quic.Connection)
	quicMutex sync.Mutex
)

func getQuicConnection(addr string, timeout time.Duration) (quic.Connection, error) {
	quicMutex.Lock()
	defer quicMutex.Unlock()

	if conn, ok := quicConns[addr]; ok {
		return conn, nil
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"hormuz-quic"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	quicConfig := &quic.Config{
		KeepAlivePeriod: 10 * time.Second,
	}

	conn, err := quic.DialAddr(ctx, addr, tlsConf, quicConfig)
	if err != nil {
		return nil, fmt.Errorf("quic dial error: %w", err)
	}

	quicConns[addr] = conn
	return conn, nil
}

func dialTransport(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := getQuicConnection(addr, timeout)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// A conexão pode ter caído. Remove do cache e tenta reconectar 1 vez.
		quicMutex.Lock()
		delete(quicConns, addr)
		quicMutex.Unlock()

		conn, err = getQuicConnection(addr, timeout)
		if err != nil {
			return nil, err
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
		defer cancel2()
		stream, err = conn.OpenStreamSync(ctx2)
		if err != nil {
			return nil, fmt.Errorf("quic stream error após reconexão: %w", err)
		}
	}

	return &quicConnWrapper{Stream: stream, conn: conn}, nil
}

type transportListener struct {
	quicListener *quic.Listener
	acceptChan   chan net.Conn
	errChan      chan error
}

func listenTransport(addr string) (*transportListener, error) {
	tlsConf := generateTLSConfig()
	l, err := quic.ListenAddr(addr, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	tl := &transportListener{
		quicListener: l,
		acceptChan:   make(chan net.Conn),
		errChan:      make(chan error),
	}
	go tl.acceptLoop()
	return tl, nil
}

func (tl *transportListener) acceptLoop() {
	for {
		conn, err := tl.quicListener.Accept(context.Background())
		if err != nil {
			tl.errChan <- err
			return
		}
		go func(c quic.Connection) {
			for {
				stream, err := c.AcceptStream(context.Background())
				if err != nil {
					return
				}
				tl.acceptChan <- &quicConnWrapper{Stream: stream, conn: c}
			}
		}(conn)
	}
}

func (tl *transportListener) Accept() (net.Conn, error) {
	select {
	case conn := <-tl.acceptChan:
		return conn, nil
	case err := <-tl.errChan:
		return nil, err
	}
}

func (tl *transportListener) Close() error {
	return tl.quicListener.Close()
}

func (tl *transportListener) Addr() net.Addr {
	return tl.quicListener.Addr()
}

type quicConnWrapper struct {
	quic.Stream
	conn quic.Connection
}

func (w *quicConnWrapper) LocalAddr() net.Addr  { return w.conn.LocalAddr() }
func (w *quicConnWrapper) RemoteAddr() net.Addr { return w.conn.RemoteAddr() }
func (w *quicConnWrapper) Close() error         { return w.Stream.Close() }

func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(crand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Hormuz"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(crand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}, NextProtos: []string{"hormuz-quic"}}
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
