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
	Type       string            `json:"type"`
	RequestID  string            `json:"request_id,omitempty"`
	Priority   int               `json:"priority,omitempty"`
	Occurrence string            `json:"occurrence,omitempty"`
	CompanyID  string            `json:"company_id,omitempty"`
	MissionID  string            `json:"mission_id,omitempty"`
	Content    string            `json:"content,omitempty"`
	Status     string            `json:"status,omitempty"`
	Payload    map[string]string `json:"payload,omitempty"`
	Queue      []QueueItem       `json:"queue,omitempty"`
}

type QueueItem struct {
	RequestID  string `json:"request_id"`
	Occurrence string `json:"occurrence"`
	Priority   int    `json:"priority"`
	GatewayID  string `json:"gateway_id"`
	Timestamp  int64  `json:"timestamp"`
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
var defaultCompanies = []string{"navio-alpha", "navio-beta", "navio-gamma", "navio-delta"}

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Variável de ambiente %s não configurada", key)
	}
	return val
}

func getAvailableGatewayConn(gateways map[string]string) (net.Conn, string, error) {
	for name, addr := range gateways {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
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
	gateways := map[string]string{
		"Norte": fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), clientPort),
		"Sul":   fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), clientPort),
		"Leste": fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), clientPort),
		"Oeste": fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), clientPort),
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
		fmt.Println("1 - Injetar Alerta Manual")
		fmt.Println("2 - Ver Status do Estreito")
		fmt.Println("3 - Ver Log de Eventos")
		fmt.Println("4 - Consultar Arrecadação (Consórcio Hormuz)")
		fmt.Println("5 - Histórico do Ledger")
		fmt.Println("0 - Sair")
		fmt.Print("Escolha uma opção (ou Enter para atualizar): ")

		choice := readChoice(reader)

		switch choice {
		case "1":
			clearScreen()
			sendManualAlert(reader, sectors, gateways)
			skipNextClear = true

		case "2":
			clearScreen()
			printStatus(sectors, gateways)
			fmt.Println()
			skipNextClear = true

		case "3":
			clearScreen()
			viewEventLog(reader, sectors, gateways)
			fmt.Println()
			skipNextClear = true

		case "4":
			clearScreen()
			viewHormuzRevenue(reader, gateways)
			fmt.Println()
			skipNextClear = true

		case "5":
			clearScreen()
			viewLedgerHistory(reader, gateways)
			fmt.Println()
			skipNextClear = true

		case "":
			clearMenuLines(13)
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

func sendManualAlert(reader *bufio.Reader, sectors []string, gateways map[string]string) {
	fmt.Println("--- INJETAR ALERTA MANUAL ---")

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

	// Economia invisível: seleciona um navio aleatório para ser cobrado no gateway
	companyID := defaultCompanies[rand.Intn(len(defaultCompanies))]
	requestID := fmt.Sprintf("client:%s:%d", setorEscolhido, time.Now().UnixNano())

	msg := Message{
		Type:       "MISSION_SUBMIT",
		RequestID:  requestID,
		Priority:   option.Priority,
		Occurrence: option.Description,
		CompanyID:  companyID,
	}

	custo := option.Priority * 10
	fmt.Printf("\n[CLIENTE] Transmitindo alerta para o setor %s...\n", setorEscolhido)
	fmt.Printf("[ECONOMIA] A fatura de %d créditos será processada nos sistemas do Consórcio Hormuz.\n", custo)

	sendWithFallback(msg, setorEscolhido, sectors, gateways)
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
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
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
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
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

// viewHormuzRevenue varre o ledger distribuído em busca de todos os pagamentos e soma o montante arrecadado.
func viewHormuzRevenue(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- ARRECADAÇÃO (CONSÓRCIO HORMUZ) ---")
	fmt.Println("Conectando ao Ledger da malha...")

	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Toda a malha está offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	// Pede um limite alto para varrer todo o histórico contábil
	msg := Message{Type: "LEDGER_REQ", Payload: map[string]string{"limit": "1000"}}
	json.NewEncoder(conn).Encode(msg)

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		fmt.Println("\n[ERRO] Falha ao ler resposta do gateway.")
		reader.ReadString('\n')
		return
	}

	var recs []map[string]interface{}
	json.Unmarshal([]byte(reply.Content), &recs)

	totalArrecadado := 0
	emissoes := 0

	for _, r := range recs {
		if t, ok := r["type"].(string); ok {
			if t == "MISSION_PAYMENT" {
				// Cada MISSION_PAYMENT tem um campo token_ids com o array de tokens gastos.
				if tokens, ok := r["token_ids"].([]interface{}); ok {
					totalArrecadado += len(tokens) * 10 // Cada token vale 10 créditos
				}
			} else if strings.HasPrefix(t, "TOKEN_MINT") {
				emissoes++
			}
		}
	}

	fmt.Println("\n=======================================================")
	fmt.Printf(" FATURAMENTO TOTAL DO CONSÓRCIO HORMUZ: %d CRÉDITOS\n", totalArrecadado)
	fmt.Println("=======================================================")
	fmt.Println("\nℹ️  Todos os créditos descontados das embarcações por")
	fmt.Println("   acionamentos de emergência foram transferidos para")
	fmt.Println("   a administração do Estreito.")
	fmt.Printf("\nEventos de emissão/recarga na malha: %d\n", emissoes)

	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

func viewLedgerHistory(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- HISTÓRICO GERAL DO LIVRO-RAZÃO ---")

	conn, gwName, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Printf("\n[ERRO] %v\n", err)
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "LEDGER_REQ", Payload: map[string]string{"limit": "30"}}
	json.NewEncoder(conn).Encode(msg)

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		fmt.Println("\n[ERRO] Falha ao ler resposta do gateway.")
		reader.ReadString('\n')
		return
	}

	var recs []map[string]interface{}
	json.Unmarshal([]byte(reply.Content), &recs)

	fmt.Printf("\n[Gateway %s] - Últimos blocos/registros na rede:\n\n", gwName)
	if len(recs) == 0 {
		fmt.Println("Nenhum registro encontrado.")
	} else {
		for i, r := range recs {
			t := ""
			if typeVal, ok := r["type"].(string); ok {
				t = typeVal
			}
			tsVal, _ := r["timestamp"].(float64)
			date := time.Unix(int64(tsVal), 0).Format("15:04:05")

			compId := ""
			if c, ok := r["company_id"].(string); ok {
				compId = c
			}

			detail := ""
			if d, ok := r["detail"].(string); ok {
				detail = d
			}

			fmt.Printf("%d. [%s] %s | Cia: %s | Detalhe: %s\n", i+1, date, t, compId, detail)
		}
	}

	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

func viewEventLog(reader *bufio.Reader, sectors []string, gateways map[string]string) {
	fmt.Println("--- LOG DE EVENTOS ---")
	fmt.Print("Quantos eventos deseja ver por setor? ")
	eventCount := readNumber(reader, 1, 20)
	fmt.Println()

	for _, sector := range sectors {
		addr := gateways[sector]
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
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
			conn, err := net.DialTimeout("tcp", target, 3*time.Second)
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
