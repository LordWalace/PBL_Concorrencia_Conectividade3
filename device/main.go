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
	"sync"
	"time"
)

type Message struct {
	Type        string            `json:"type"`
	DroneID     string            `json:"drone_id"`
	Content     string            `json:"content,omitempty"`
	Status      string            `json:"status,omitempty"`
	MissionInfo string            `json:"mission_info,omitempty"`
	Occurrence  string            `json:"occurrence,omitempty"`
	Payload     map[string]string `json:"payload,omitempty"`
}

var (
	droneID            string
	setorID            string
	deviceIP           string
	deviceHost         string
	deviceControlPort  string
	gatewayAddrs       []string
	gatewayNames       []string
	stateMutex         sync.Mutex
	currentGatewayIdx  int = -1
	currentGateway     string
	currentGatewayName string
	statusValue        string = "DISPONIVEL"
	missionActive      bool
	missionEnd         time.Time
	missionDuration    time.Duration
	missionDescription string
)

// mustEnv valida que a variavel de ambiente exista antes da inicializacao do servico.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("[FATAL] Variável de ambiente obrigatória ausente: %s", key)
	}
	return v
}

// mustDurationEnv le duracao de missao de forma segura e consistente.
func mustDurationEnv(key string) time.Duration {
	v := mustEnv(key)
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if sec, err := strconv.Atoi(v); err == nil {
		return time.Duration(sec) * time.Second
	}
	log.Fatalf("[FATAL] Variável de ambiente inválida para %s: %q", key, v)
	return 0
}

// main inicializa o servico e os componentes de rede, garantindo sincronizacao e redundancia entre gateways.
func main() {
	rand.Seed(time.Now().UnixNano())
	droneID = mustEnv("DEVICE_ID")
	deviceIP = mustEnv("DEVICE_IP")
	deviceHost = mustEnv("DEVICE_HOST")
	deviceControlPort = mustEnv("DEVICE_CONTROL_PORT")
	setorID = mustEnv("SETOR_ID")

	gatewayNames = []string{"Norte", "Sul", "Leste", "Oeste"}
	gatewayAddrs = []string{
		fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), mustEnv("GATEWAY_TCP_REG_PORT")),
		fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), mustEnv("GATEWAY_TCP_REG_PORT")),
		fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), mustEnv("GATEWAY_TCP_REG_PORT")),
		fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), mustEnv("GATEWAY_TCP_REG_PORT")),
	}
	missionDuration = mustDurationEnv("MISSION_DURATION")

	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	log.Printf("%s Iniciando device", logPrefix)

	preferredIndex := locatePreferredGatewayBySector(setorID)
	if preferredIndex < 0 {
		preferredIndex = locatePreferredGatewayByIP(deviceIP)
	}
	if preferredIndex >= 0 {
		log.Printf("%s Gateway preferencial identificado: %s (%s)", logPrefix, gatewayNames[preferredIndex], gatewayAddrs[preferredIndex])
	}

	myControlAddr := fmt.Sprintf("%s:%s", deviceIP, deviceControlPort)

	go registerLoop(myControlAddr, preferredIndex)
	go heartbeatLoop(myControlAddr)
	startCommandListener(myControlAddr)
}

// locatePreferredGatewayBySector identifica o gateway preferencial para o setor do drone.
func locatePreferredGatewayBySector(setorID string) int {
	for i, name := range gatewayNames {
		if strings.EqualFold(name, setorID) {
			return i
		}
	}
	return -1
}

// locatePreferredGatewayByIP escolhe gateway com base no IP de rede do drone.
func locatePreferredGatewayByIP(deviceIP string) int {
	ips := []string{mustEnv("IP_NORTE"), mustEnv("IP_SUL"), mustEnv("IP_LESTE"), mustEnv("IP_OESTE")}
	for i, ip := range ips {
		if ip == deviceIP {
			return i
		}
	}
	return 0
}

// registerLoop tenta registrar o drone no gateway ate obter resposta bem sucedida.
func registerLoop(controlAddr string, preferredIndex int) {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	for {
		if registerToGateway(controlAddr, preferredIndex) == nil {
			return
		}
		log.Printf("%s Registro inicial falhou, tentando novamente em 5s", logPrefix)
		time.Sleep(5 * time.Second)
	}
}

// registerToGateway realiza registro ordenado no gateway preferido e fallback em caso de indisponibilidade.
func registerToGateway(controlAddr string, preferredIndex int) error {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	if preferredIndex < 0 || preferredIndex >= len(gatewayAddrs) {
		preferredIndex = 0
	}

	preferredName := gatewayNames[preferredIndex]
	log.Printf("%s Tentando registrar primeiramente no gateway preferido %s", logPrefix, preferredName)

	const preferredAttempts = 5
	for attempt := 1; attempt <= preferredAttempts; attempt++ {
		if tryRegisterGateway(preferredIndex, controlAddr) == nil {
			return nil
		}
		log.Printf("%s Gateway preferido %s indisponível, tentativa %d/%d", logPrefix, preferredName, attempt, preferredAttempts)
		time.Sleep(2 * time.Second)
	}

	log.Printf("%s Gateway preferido %s não respondeu após %d tentativas. Tentando fallback para outros gateways.", logPrefix, preferredName, preferredAttempts)
	order := gatewayOrder(preferredIndex)
	for _, idx := range order {
		if idx == preferredIndex {
			continue
		}
		if tryRegisterGateway(idx, controlAddr) == nil {
			return nil
		}
	}

	log.Printf("%s Nenhum gateway disponível para registro", logPrefix)
	return fmt.Errorf("nenhum gateway disponível")
}

// gatewayOrder define a ordem de tentativa de gateways para fallback seguro.
func gatewayOrder(preferredIndex int) []int {
	order := make([]int, 0, len(gatewayAddrs))
	if preferredIndex < 0 || preferredIndex >= len(gatewayAddrs) {
		preferredIndex = 0
	}
	for i := 0; i < len(gatewayAddrs); i++ {
		order = append(order, (preferredIndex+i)%len(gatewayAddrs))
	}
	return order
}

// tryRegisterGateway faz o envio do registro do drone a um gateway especifico.
func tryRegisterGateway(idx int, controlAddr string) error {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	addr := gatewayAddrs[idx]
	name := gatewayNames[idx]
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		log.Printf("%s Falha ao conectar no gateway %s (%s): %v", logPrefix, name, addr, err)
		return err
	}
	defer conn.Close()

	msg := Message{
		Type:        "DEVICE_REG",
		DroneID:     droneID,
		Content:     controlAddr,
		Status:      currentStatus(),
		MissionInfo: currentMissionInfo(),
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		log.Printf("%s Falha ao registrar no gateway %s: %v", logPrefix, name, err)
		return err
	}

	stateMutex.Lock()
	currentGatewayIdx = idx
	currentGateway = addr
	currentGatewayName = name
	stateMutex.Unlock()

	log.Printf("%s Registrado com sucesso no gateway %s (%s). Status atual: %s", logPrefix, name, addr, currentStatus())
	if missionActive {
		log.Printf("%s MIGRACAO: drone migrou durante missão ativa para %s", logPrefix, name)
	}
	return nil
}

// heartbeatLoop envia batimentos periodicos ao gateway e aciona migracao se o gateway falhar.
func heartbeatLoop(controlAddr string) {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	for {
		time.Sleep(time.Duration(3+rand.Intn(3)) * time.Second)
		log.Printf("%s [HEARTBEAT] Enviando heartbeat", logPrefix)
		if err := sendHeartbeat(); err != nil {
			log.Printf("%s [HEARTBEAT] Falha no heartbeat: %v", logPrefix, err)
			log.Printf("%s [MIGRACAO] Detectado gateway offline. Iniciando migração.", logPrefix)
			migrateGateway(controlAddr)
		}
	}
}

// sendHeartbeat envia heartbeat e valida ACK do gateway para manter a sessao viva.
func sendHeartbeat() error {
	stateMutex.Lock()
	addr := currentGateway
	status := statusValue
	missionInfo := currentMissionInfo()
	stateMutex.Unlock()

	if addr == "" {
		return fmt.Errorf("sem gateway atual")
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := Message{
		Type:        "HEARTBEAT",
		DroneID:     droneID,
		Content:     fmt.Sprintf("%s:%s", deviceIP, deviceControlPort),
		Status:      status,
		MissionInfo: missionInfo,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return err
	}

	var ack Message
	if err := json.NewDecoder(conn).Decode(&ack); err != nil {
		return err
	}
	if ack.Type != "HEARTBEAT_ACK" {
		return fmt.Errorf("resposta inesperada do gateway: %s", ack.Type)
	}

	log.Printf("[DRONE/%s] [HEARTBEAT] Heartbeat enviado ao gateway %s", droneID, addr)
	return nil
}

// migrateGateway realiza fallback para outro gateway em caso de perda de conectividade.
func migrateGateway(controlAddr string) {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	currentState := currentStatus()
	activeDuringMigration := missionActive
	fromName, fromAddr := currentGatewayName, currentGateway
	preferred := currentGatewayIdx
	order := gatewayOrder(preferred)

	for _, idx := range order {
		if idx == currentGatewayIdx {
			continue
		}
		if tryRegisterGateway(idx, controlAddr) == nil {
			log.Printf("%s [MIGRACAO] Migração bem-sucedida de %s (%s) para %s (%s)", logPrefix, fromName, fromAddr, gatewayNames[idx], gatewayAddrs[idx])
			if activeDuringMigration {
				log.Printf("%s [MIGRACAO] drone migrou durante missão ativa", logPrefix)
				log.Printf("%s [MIGRACAO] novo gateway informado de que o drone está %s", logPrefix, currentState)
			}
			return
		}
	}

	log.Printf("%s [MIGRACAO] Nenhum gateway alternativo disponível, aguardando 5s", logPrefix)
	time.Sleep(5 * time.Second)
}

// startCommandListener abre listener de comandos de despacho para o drone.
func startCommandListener(controlAddr string) {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	listenerAddr := controlAddr
	listener, err := net.Listen("tcp", listenerAddr)
	if err != nil {
		log.Fatalf("%s Falha ao iniciar listener de comando: %v", logPrefix, err)
	}
	log.Printf("%s [COMANDO] Escutando comandos em %s", logPrefix, listenerAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleCommand(conn)
	}
}

// handleCommand processa comandos recebidos do gateway e atualiza o estado da missao.
func handleCommand(conn net.Conn) {
	defer conn.Close()
	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		log.Printf("[DRONE/%s] [FALHA] Erro ao decodificar comando: %v", droneID, err)
		return
	}

	if msg.Type != "DISPATCH" {
		log.Printf("[DRONE/%s] [COMANDO] Comando desconhecido: %s", droneID, msg.Type)
		return
	}

	stateMutex.Lock()
	missionDescription = msg.Occurrence
	stateMutex.Unlock()

	log.Printf("[DRONE/%s] [MISSAO] DISPATCH recebido: %s", droneID, msg.Occurrence)
	startMission()
}

// startMission inicia a missao do drone e mantem o estado ate conclusao.
func startMission() {
	stateMutex.Lock()
	if missionActive {
		log.Printf("[DRONE/%s] [MISSAO] Já em missão ativa, ignorando novo dispatch", droneID)
		stateMutex.Unlock()
		return
	}
	missionActive = true
	statusValue = "OCUPADO"
	duration := missionDuration
	if duration <= 0 {
		duration = 20 * time.Second
	}
	missionEnd = time.Now().Add(duration)
	gatewayName := currentGatewayName
	stateMutex.Unlock()

	log.Printf("[DRONE/%s] [MISSAO] Iniciando missão de %s no gateway %s", droneID, duration, gatewayName)
	go func() {
		startTime := time.Now()
		time.Sleep(duration)

		stateMutex.Lock()
		missionActive = false
		statusValue = "DISPONIVEL"
		missionDescription = ""
		stateMutex.Unlock()

		elapsed := time.Since(startTime)
		log.Printf("[DRONE/%s] [MISSAO] Missão concluída após %s", droneID, elapsed)
		sendRelease()
	}()
}

// sendDroneFailureNotification notifica o gateway sobre falhas criticas do drone.
func sendDroneFailureNotification(reason string) {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	msg := Message{
		Type:        "DRONE_FAILED",
		DroneID:     droneID,
		Content:     reason,
		Status:      currentStatus(),
		MissionInfo: currentMissionInfo(),
	}

	for _, addr := range gatewayAddrs {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			log.Printf("%s [FALHA] Não foi possível notificar gateway %s: %v", logPrefix, addr, err)
			continue
		}
		if err := json.NewEncoder(conn).Encode(msg); err != nil {
			log.Printf("%s [FALHA] Erro ao notificar gateway %s: %v", logPrefix, addr, err)
		}
		conn.Close()
		log.Printf("%s [FALHA] Notificação de falha enviada para %s", logPrefix, addr)
		return
	}
	log.Printf("%s [FALHA] Não foi possível notificar nenhum gateway; continuarei tentando junto ao gateway atual", logPrefix)
}

// sendRelease libera o drone ao gateway ao finalizar uma missao ou operacao.
func sendRelease() {
	logPrefix := fmt.Sprintf("[DRONE/%s]", droneID)
	for {
		stateMutex.Lock()
		addr := currentGateway
		stateMutex.Unlock()

		if addr == "" {
			log.Printf("%s [MISSAO] Sem gateway atual para enviar RELEASE, aguardando migração", logPrefix)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("%s [MISSAO] Enviando RELEASE ao gateway %s", logPrefix, addr)
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			log.Printf("%s [MISSAO] Falha ao conectar para RELEASE: %v", logPrefix, err)
			migrateGateway(fmt.Sprintf("%s:%s", deviceHost, deviceControlPort))
			continue
		}

		msg := Message{Type: "RELEASE", DroneID: droneID}
		if err := json.NewEncoder(conn).Encode(&msg); err != nil {
			log.Printf("%s [MISSAO] Erro ao enviar RELEASE: %v", logPrefix, err)
			conn.Close()
			migrateGateway(fmt.Sprintf("%s:%s", deviceHost, deviceControlPort))
			continue
		}
		conn.Close()
		log.Printf("%s [MISSAO] RELEASE enviado com sucesso ao gateway %s", logPrefix, addr)
		return
	}
}

// currentStatus retorna o status operacional atual do drone para o heartbeat.
func currentStatus() string {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	return statusValue
}

// currentMissionInfo retorna a descricao de missao atual usada no heartbeat.
func currentMissionInfo() string {
	stateMutex.Lock()
	defer stateMutex.Unlock()
	if missionActive {
		if missionDescription != "" {
			return missionDescription
		}
		return fmt.Sprintf("em missão até %s", missionEnd.Format(time.RFC3339))
	}
	return "sem missão"
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
