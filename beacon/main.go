package main

import (
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
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

// --- QUIC TRANSPORT ABSTRACTION ---

var (
	quicConns = make(map[string]quic.Connection)
	quicMutex sync.Mutex
)

func getQuicConnection(addr string, timeout time.Duration) (quic.Connection, error) {
	quicMutex.Lock()
	if conn, ok := quicConns[addr]; ok {
		quicMutex.Unlock()
		return conn, nil
	}
	quicMutex.Unlock()

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

	quicMutex.Lock()
	if existing, ok := quicConns[addr]; ok {
		quicMutex.Unlock()
		conn.CloseWithError(0, "")
		return existing, nil
	}
	quicConns[addr] = conn
	quicMutex.Unlock()

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

