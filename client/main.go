package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
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

// mustEnv valida que a variavel de ambiente exista antes da inicializacao do servico.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("[FATAL] Variável de ambiente obrigatória ausente: %s", key)
	}
	return v
}

// main inicializa o servico e os componentes de rede, garantindo sincronizacao e redundancia entre gateways.
func main() {
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
		fmt.Println("0 - Sair")
		fmt.Print("Escolha uma opção (ou Enter para atualizar): ")

		choice := readChoice(reader)

		switch choice {
		case "1":
			clearScreen()
			sendManualAlert(reader, sectors, gateways)
			time.Sleep(2 * time.Second)
			clearScreen()

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

		case "":
			clearMenuLines(11)
			continue

		case "0":
			clearScreen()
			fmt.Println("Encerrando cliente.")
			return

		default:
			clearMenuLines(11)
			continue
		}
	}
}

// sendManualAlert coleta e envia alertas manuais com fallback entre gateways para confiabilidade.
func sendManualAlert(reader *bufio.Reader, sectors []string, gateways map[string]string) {
	fmt.Println("--- INJETAR ALERTA MANUAL ---")
	for i, setor := range sectors {
		fmt.Printf("%d - %s\n", i+1, setor)
	}
	fmt.Print("Selecione o setor (1-4): ")
	sectorIndex := readNumber(reader, 1, len(sectors)) - 1
	setorEscolhido := sectors[sectorIndex]

	fmt.Println("\nOcorrências disponíveis:")
	for i, option := range occurrenceOptions {
		fmt.Printf("%d - %s (Prioridade %d)\n", i+1, option.Description, option.Priority)
	}
	fmt.Print("Escolha a ocorrência (1-7): ")
	occurrenceIndex := readNumber(reader, 1, len(occurrenceOptions)) - 1
	option := occurrenceOptions[occurrenceIndex]

	requestID := fmt.Sprintf("client:%s:%d", setorEscolhido, time.Now().UnixNano())
	msg := Message{
		Type:       "ALERT",
		RequestID:  requestID,
		Priority:   option.Priority,
		Occurrence: option.Description,
	}

	fmt.Printf("\n[CLIENTE] Enviando alerta para %s com prioridade %d e ocorrência '%s'\n", setorEscolhido, option.Priority, option.Description)
	sendWithFallback(msg, setorEscolhido, sectors, gateways)
}

// occurrencesByPriority agrupa ocorrencias por prioridade para selecao de alertas criticos.
func occurrencesByPriority(priority int) []OccurrenceOption {
	filtered := make([]OccurrenceOption, 0, len(occurrenceOptions))
	for _, option := range occurrenceOptions {
		if option.Priority == priority {
			filtered = append(filtered, option)
		}
	}
	return filtered
}

// printStatus consulta gateways e apresenta status consolidado de drones e fila.
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
				for i, item := range reply.Queue {
					sectorResults[idx] += fmt.Sprintf("\n      %d) %s | P%d | Origem: %s | %s", i+1, item.Occurrence, item.Priority, item.GatewayID, time.Unix(item.Timestamp, 0).Format("15:04:05"))
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
		return
	}

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

// cleanDroneName normaliza identificadores de drone para visualizacao unificada.
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

// viewEventLog exibe o historico de eventos de forma ordenada para analise operacional.
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
}

// readChoice le a opcao de menu do usuario de forma robusta.
func readChoice(reader *bufio.Reader) string {
	line, err := reader.ReadString('\n')
	if err != nil {
		time.Sleep(2 * time.Second)
		os.Exit(1)
		return ""
	}
	// Apenas retorna a string limpa. Se for inválido, o switch do main lida com isso apagando o menu de forma limpa.
	return strings.TrimSpace(line)
}

// readNumber valida entrada numerica de menu com limites definidos.
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

// sendWithFallback garante envio de alerta com retry e fallback entre gateways ativos.
func sendWithFallback(msg Message, initialSector string, sectors []string, gateways map[string]string) {
	order := make([]string, 0, len(sectors))
	order = append(order, initialSector)
	for _, sector := range sectors {
		if sector != initialSector {
			order = append(order, sector)
		}
	}

	for {
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
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			var ack Message
			if err := json.NewDecoder(conn).Decode(&ack); err != nil {
				conn.Close()
				continue
			}
			conn.Close()
			if ack.Type != "ALERT_ACK" {
				continue
			}
			fmt.Printf("[CLIENTE] Alerta salvo com sucesso no Setor %s (%s)\n", sector, target)
			return
		}

		fmt.Println("[CLIENTE] Toda a malha está offline. Tentando novamente em 5 segundos...")
		time.Sleep(5 * time.Second)
	}
}

// clearScreen executa sua responsabilidade no fluxo distribuido de forma deterministica e confiavel.
func clearScreen() {
	fmt.Print("\033[H\033[2J\033[3J")
}

// clearMenuLines executa sua responsabilidade no fluxo distribuido de forma deterministica e confiavel.
func clearMenuLines(linhas int) {
	// Nova função mágica: sobe o cursor 'N' linhas e apaga tudo abaixo dele
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
