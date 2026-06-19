package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

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

type Message struct {
	Type       string `json:"type"`
	RequestID  string `json:"request_id,omitempty"`
	Priority   int    `json:"priority,omitempty"`
	Occurrence string `json:"occurrence,omitempty"`
	GatewayID  string `json:"gateway_id,omitempty"`
}

// mustEnv valida que a variavel de ambiente exista antes da inicializacao do servico.
func envOrDefault(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func mustEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("[FATAL] Variável de ambiente obrigatória ausente: %s", key)
	}
	return value
}

// getEnvInt le inteiros de ambiente com fallback seguro.
func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// main inicializa o servico e os componentes de rede, garantindo sincronizacao e redundancia entre gateways.
func main() {
	setorID := mustEnv("SETOR_ID")
	seed := time.Now().UnixNano()
	for _, b := range setorID {
		seed += int64(b)
	}
	rand.Seed(seed)
	r := rand.New(rand.NewSource(seed))

	localGatewayIP := envOrDefault("GATEWAY_IP", mustEnv("IP_"+strings.ToUpper(mustEnv("SETOR_ID"))))
	gatewayPort := mustEnv("GATEWAY_TCP_REG_PORT")

	allGateways := []string{
		fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), gatewayPort),
		fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), gatewayPort),
		fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), gatewayPort),
		fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), gatewayPort),
	}

	primaryGateway := fmt.Sprintf("%s:%s", localGatewayIP, gatewayPort)
	backupGateways := make([]string, 0, len(allGateways)-1)
	for _, addr := range allGateways {
		if addr == primaryGateway {
			continue
		}
		backupGateways = append(backupGateways, addr)
	}

	log.Printf("[BEACON/%s] Beacon iniciado com gateway primário %s", setorID, primaryGateway)

	go generateOccurrencesLoop(r, setorID, primaryGateway, backupGateways)

	select {}
}

// generateOccurrencesLoop fabrica ocorrencias de sensor e aciona envio com failover de gateway.
func generateOccurrencesLoop(r *rand.Rand, beaconID, primaryGateway string, backupGateways []string) {
	minInterval := getEnvInt("BEACON_MIN_INTERVAL", 5)
	maxInterval := getEnvInt("BEACON_MAX_INTERVAL", 15)
	if maxInterval < minInterval {
		maxInterval = minInterval
	}

	for {
		interval := maxInterval - minInterval + 1
		sleepSeconds := r.Intn(interval) + minInterval
		time.Sleep(time.Duration(sleepSeconds) * time.Second)

		index := r.Intn(len(occurrenceOptions))
		option := occurrenceOptions[index]

		msg := Message{
			Type:       "ALERT",
			RequestID:  fmt.Sprintf("beacon:%s:%d", beaconID, time.Now().UnixNano()),
			Priority:   option.Priority,
			Occurrence: option.Description,
			GatewayID:  beaconID,
		}

		sendWithFailover(msg, primaryGateway, backupGateways)
	}
}

// sendWithFailover realiza envio de alerta para gateway primario e alterna para backups se houver falha.
func sendWithFailover(msg Message, primaryGateway string, backupGateways []string) {
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[BEACON/%s] Falha ao serializar alerta: %v", msg.GatewayID, err)
		return
	}

	for {
		if trySendToGateway(primaryGateway, payload) {
			return
		}

		for _, backup := range backupGateways {
			if trySendToGateway(backup, payload) {
				return
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// trySendToGateway tenta enviar payload TCP ao gateway e detecta falha de conexao.
func trySendToGateway(addr string, payload []byte) bool {
	conn, err := dialTransport(addr, 3*time.Second)
	if err != nil {
		log.Printf("[BEACON] Falha ao conectar em %s: %v", addr, err)
		return false
	}
	defer conn.Close()

	_, err = conn.Write(payload)
	if err != nil {
		log.Printf("[BEACON] Falha ao enviar payload para %s: %v", addr, err)
		return false
	}

	log.Printf("[BEACON] Alerta enviado com sucesso para %s", addr)
	return true
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

// --- TCP TRANSPORT ABSTRACTION ---

func dialTransport(addr string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("tcp dial error: %w", err)
	}
	return conn, nil
}

func listenTransport(addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return l, nil
}
