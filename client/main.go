package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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

// mustEnv valida a existencia do parametro vital ou aborta a execucao.
func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Variável de ambiente %s não configurada", key)
	}
	return val
}

// getAvailableGatewayConn obtem e retorna a informacao solicitada.
func getAvailableGatewayConn(gateways map[string]string) (net.Conn, string, error) {
	for name, addr := range gateways {
		conn, err := dialTransport(addr, 1*time.Second)
		if err == nil {
			return conn, name, nil
		}
	}
	return nil, "", fmt.Errorf("nenhum gateway disponível no momento")
}

// main inicializa o servico, as dependencias e os workers principais.
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

// sendManualAlert realiza o envio da mensagem ou pacote pela rede.
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

// solveProblemMenu executa as rotinas de controle e processamento especificas desta rotina.
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

		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
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

// dispatchClientMission executa as rotinas de controle e processamento especificas desta rotina.
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

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
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

// printStatus exibe as informacoes e menus na interface de usuario.
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
			conn, err := dialTransport(addr, 1*time.Second)
			if err != nil {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] OFFLINE (dial err: %v)", setor, err)
				return
			}
			defer conn.Close()

			if err := json.NewEncoder(conn).Encode(Message{Type: "STATUS_REQ"}); err != nil {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] Erro ao solicitar status: %v", setor, err)
				return
			}

			var reply Message
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if err := json.NewDecoder(conn).Decode(&reply); err != nil {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] OFFLINE (decode err: %v)", setor, err)
				return
			}
			if reply.Type != "STATUS_REP" {
				sectorResults[idx] = fmt.Sprintf("[Setor %s] OFFLINE (bad type: %s)", setor, reply.Type)
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

// viewEventLog executa as rotinas de controle e processamento especificas desta rotina.
func viewEventLog(reader *bufio.Reader, sectors []string, gateways map[string]string) {
	fmt.Println("--- LOG DE EVENTOS ---")
	fmt.Print("Quantos eventos deseja ver por setor? ")
	eventCount := readNumber(reader, 1, 20)
	fmt.Println()

	for _, sector := range sectors {
		addr := gateways[sector]
		conn, err := dialTransport(addr, 1*time.Second)
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
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
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

// readChoice realiza a leitura de dados do terminal ou stream.
func readChoice(reader *bufio.Reader) string {
	line, err := reader.ReadString('\n')
	if err != nil {
		time.Sleep(2 * time.Second)
		os.Exit(1)
		return ""
	}
	return strings.TrimSpace(line)
}

// readNumber realiza a leitura de dados do terminal ou stream.
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

// sendWithFallback realiza o envio da mensagem ou pacote pela rede.
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
			conn, err := dialTransport(target, 1*time.Second)
			if err != nil {
				continue
			}
			if err := json.NewEncoder(conn).Encode(msg); err != nil {
				conn.Close()
				continue
			}
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
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

// cleanDroneName executa as rotinas de controle e processamento especificas desta rotina.
func cleanDroneName(droneID string) string {
	droneID = strings.TrimPrefix(droneID, "drone_")
	droneID = strings.TrimPrefix(droneID, "Drone_")
	return strings.ReplaceAll(droneID, "_", "-")
}

// capitalizeFirst executa as rotinas de controle e processamento especificas desta rotina.
func capitalizeFirst(text string) string {
	if len(text) == 0 {
		return text
	}
	return strings.ToUpper(string(text[0])) + text[1:]
}

// formatDroneName executa as rotinas de controle e processamento especificas desta rotina.
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

// clearScreen limpa a tela ou formatacao visual do terminal interativo.
func clearScreen() {
	fmt.Print("\033[H\033[2J\033[3J")
}

// clearMenuLines limpa a tela ou formatacao visual do terminal interativo.
func clearMenuLines(linhas int) {
	fmt.Printf("\033[%dA\033[J", linhas)
}

// --- TCP TRANSPORT ABSTRACTION ---

// dialTransport estabelece uma nova conexao de rede TCP ou QUIC.
func dialTransport(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("tcp dial error: %w", err)
	}
	return conn, nil
}

// listenTransport abre uma porta local e escuta por novas conexoes.
func listenTransport(addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return l, nil
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
