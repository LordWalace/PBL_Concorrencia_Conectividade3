package main

import (
	"bufio"
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

type Message struct {
	Type      string            `json:"type"`
	DroneID   string            `json:"drone_id,omitempty"`
	GatewayID string            `json:"gateway_id,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
	Priority  int               `json:"priority,omitempty"`
	Lamport   int               `json:"lamport,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	Payload   map[string]string `json:"payload,omitempty"`
	Content   string            `json:"content,omitempty"`
	Status    string            `json:"status,omitempty"`
	CompanyID string            `json:"company_id,omitempty"`
}

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
		conn, err := dialTransport(addr, 3*time.Second)
		if err == nil {
			return conn, name, nil
		}
	}
	return nil, "", fmt.Errorf("nenhum gateway disponível no momento")
}

// getCompanyList obtem e retorna a informacao solicitada.
func getCompanyList(gateways map[string]string) ([]string, error) {
	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	msg := Message{Type: "COMPANY_LIST_REQ"}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		return nil, err
	}
	if reply.Type != "COMPANY_LIST_REP" {
		return nil, fmt.Errorf("invalid reply")
	}

	var list []string
	json.Unmarshal([]byte(reply.Content), &list)
	return list, nil
}

// selectCompany executa as rotinas de controle e processamento especificas desta rotina.
func selectCompany(reader *bufio.Reader, gateways map[string]string) string {
	fmt.Println("Conectando ao gateway para obter lista de empresas...")
	list, err := getCompanyList(gateways)
	if err != nil || len(list) == 0 {
		fmt.Println("[ERRO] Nenhuma empresa cadastrada ou malha offline.")
		return ""
	}
	fmt.Println("\nSelecione a empresa:")
	for i, c := range list {
		fmt.Printf("%d - %s\n", i+1, c)
	}
	fmt.Println("0 - Voltar")
	fmt.Print("\nEscolha: ")

	for {
		line, _ := reader.ReadString('\n')
		idx, err := strconv.Atoi(strings.TrimSpace(line))
		if err == nil {
			if idx == 0 {
				return ""
			}
			if idx > 0 && idx <= len(list) {
				return list[idx-1]
			}
		}
		fmt.Print("Entrada inválida. Escolha: ")
	}
}

// main inicializa o servico, as dependencias e os workers principais.
func main() {
	rand.Seed(time.Now().UnixNano())

	clientPort := mustEnv("GATEWAY_TCP_CLIENT_PORT")
	gateways := map[string]string{
		"Norte": fmt.Sprintf("%s:%s", mustEnv("IP_NORTE"), clientPort),
		"Sul":   fmt.Sprintf("%s:%s", mustEnv("IP_SUL"), clientPort),
		"Leste": fmt.Sprintf("%s:%s", mustEnv("IP_LESTE"), clientPort),
		"Oeste": fmt.Sprintf("%s:%s", mustEnv("IP_OESTE"), clientPort),
	}

	reader := bufio.NewReader(os.Stdin)
	skipNextClear := false

	for {
		if !skipNextClear {
			clearScreen()
		} else {
			skipNextClear = false
		}

		fmt.Println("======================================")
		fmt.Println("  ADMINISTRAÇÃO DO CONSÓRCIO HORMUZ")
		fmt.Println("======================================")
		fmt.Println("\nMenu Admin:")
		fmt.Println("1 - Cadastrar Nova Empresa/Navio")
		fmt.Println("2 - Adicionar Créditos Manualmente")
		fmt.Println("3 - Consultar Saldo da Empresa")
		fmt.Println("4 - Consultar Arrecadação do Consórcio Hormuz")
		fmt.Println("5 - Histórico e Auditoria")
		fmt.Println("0 - Sair")
		fmt.Print("Escolha uma opção (ou Enter para atualizar): ")

		choice := readChoice(reader)

		switch choice {
		case "1":
			clearScreen()
			adminRegisterCompany(reader, gateways)
			skipNextClear = true

		case "2":
			clearScreen()
			adminCredit(reader, gateways)
			skipNextClear = true

		case "3":
			clearScreen()
			adminBalance(reader, gateways)
			skipNextClear = true

		case "4":
			clearScreen()
			adminRevenue(reader, gateways)
			skipNextClear = true

		case "5":
			clearScreen()
			adminAudit(reader, gateways)
			skipNextClear = true

		case "":
			clearMenuLines(13)
			continue

		case "0":
			clearScreen()
			fmt.Println("Encerrando admin.")
			return

		default:
			clearMenuLines(13)
			continue
		}
	}
}

// adminRegisterCompany executa as rotinas de controle e processamento especificas desta rotina.
func adminRegisterCompany(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- CADASTRAR NOVA EMPRESA / NAVIO ---")
	fmt.Print("Digite o nome/ID da nova empresa (ou 0 para cancelar): ")
	companyID := readChoice(reader)
	if companyID == "0" || companyID == "" {
		return
	}

	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "COMPANY_REG", CompanyID: companyID}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err == nil && reply.Type == "COMPANY_ACK" {
		fmt.Printf("\n[SUCESSO] Empresa %s cadastrada com sucesso!\n", companyID)
	} else {
		fmt.Println("\n[ERRO] Falha ao cadastrar.")
	}
	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

// adminCredit executa as rotinas de controle e processamento especificas desta rotina.
func adminCredit(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- ADICIONAR TOKENS MANUALMENTE ---")
	companyID := selectCompany(reader, gateways)
	if companyID == "" {
		return
	}

	fmt.Printf("\nQuantos tokens deseja adicionar à empresa '%s'? (1 token = 10 créditos): ", companyID)
	amountStr := readChoice(reader)
	amount, err := strconv.Atoi(amountStr)
	if err != nil || amount <= 0 {
		fmt.Println("Valor inválido.")
		reader.ReadString('\n')
		return
	}

	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "ADMIN_CREDIT", CompanyID: companyID, Payload: map[string]string{"amount": amountStr}}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err == nil && reply.Type == "ADMIN_CREDIT_ACK" {
		if reply.Status == "OK" {
			fmt.Printf("\n[SUCESSO] %s\n", reply.Content)
		} else {
			fmt.Printf("\n[ERRO] %s\n", reply.Content)
		}
	} else {
		fmt.Println("\n[ERRO] Falha ao comunicar crédito manual.")
	}
	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

// adminBalance executa as rotinas de controle e processamento especificas desta rotina.
func adminBalance(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- SALDO DA EMPRESA ---")
	companyID := selectCompany(reader, gateways)
	if companyID == "" {
		return
	}

	conn, gwName, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "BALANCE_REQ", CompanyID: companyID}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err == nil && reply.Type == "BALANCE_REP" {
		fmt.Printf("\n[Gateway %s] Saldo da empresa %s: %s tokens (%s créditos)\n", gwName, companyID, reply.Payload["active_tokens"], reply.Payload["active_credits"])
	} else {
		fmt.Println("\n[ERRO] Falha ao consultar saldo.")
	}
	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

// adminRevenue executa as rotinas de controle e processamento especificas desta rotina.
func adminRevenue(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("--- ARRECADAÇÃO DO CONSÓRCIO HORMUZ ---")
	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Toda a malha está offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "REVENUE_REQ"}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err == nil && reply.Type == "REVENUE_REP" {
		total := reply.Payload["total_arrecadado"]
		emissoes := reply.Payload["emissoes"]
		fmt.Println("\n=======================================================")
		fmt.Printf(" FATURAMENTO TOTAL DO CONSÓRCIO HORMUZ: %s CRÉDITOS\n", total)
		fmt.Println("=======================================================")
		fmt.Println("\nℹ️  Todos os créditos descontados das embarcações por")
		fmt.Println("   acionamentos de emergência.")
		fmt.Printf("\nEventos de emissão na malha (Inicial/Periódico/Admin): %s\n", emissoes)
	} else {
		fmt.Println("\n[ERRO] Falha ao consultar arrecadação.")
	}

	fmt.Println("\nPressione Enter para voltar ao menu...")
	reader.ReadString('\n')
}

// adminAudit executa as rotinas de controle e processamento especificas desta rotina.
func adminAudit(reader *bufio.Reader, gateways map[string]string) {
	for {
		clearScreen()
		fmt.Println("--- HISTÓRICO E AUDITORIA ---")
		fmt.Println("1 - Ver Ledger Global")
		fmt.Println("2 - Ver Ledger por Empresa")
		fmt.Println("3 - Ver Tokens Gastos por Empresa")
		fmt.Println("0 - Voltar")
		fmt.Print("Escolha: ")

		choice := readChoice(reader)
		switch choice {
		case "1":
			adminLedgerGlobal(reader, gateways)
		case "2":
			adminLedgerCompany(reader, gateways)
		case "3":
			adminSpentTokens(reader, gateways)
		case "0":
			return
		}
	}
}

// adminLedgerGlobal executa as rotinas de controle e processamento especificas desta rotina.
func adminLedgerGlobal(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("\n--- LEDGER GLOBAL ---")
	conn, gwName, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "LEDGER_GLOBAL_REQ"}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		fmt.Println("\n[ERRO] Falha ao ler resposta.")
		reader.ReadString('\n')
		return
	}

	var recs []map[string]interface{}
	json.Unmarshal([]byte(reply.Content), &recs)

	fmt.Printf("\n[Gateway %s] - Últimos blocos globais:\n\n", gwName)
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
	fmt.Println("\nPressione Enter para voltar...")
	reader.ReadString('\n')
}

// adminLedgerCompany executa as rotinas de controle e processamento especificas desta rotina.
func adminLedgerCompany(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("\n--- LEDGER POR EMPRESA ---")
	companyID := selectCompany(reader, gateways)
	if companyID == "" {
		return
	}
	conn, gwName, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "LEDGER_REQ", CompanyID: companyID, Payload: map[string]string{"limit": "50"}}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		fmt.Println("\n[ERRO] Falha ao ler resposta.")
		reader.ReadString('\n')
		return
	}
	var recs []map[string]interface{}
	json.Unmarshal([]byte(reply.Content), &recs)

	fmt.Printf("\n[Gateway %s] - Blocos da empresa %s:\n\n", gwName, companyID)
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
			detail := ""
			if d, ok := r["detail"].(string); ok {
				detail = d
			}
			fmt.Printf("%d. [%s] %s | Detalhe: %s\n", i+1, date, t, detail)
		}
	}
	fmt.Println("\nPressione Enter para voltar...")
	reader.ReadString('\n')
}

// adminSpentTokens executa as rotinas de controle e processamento especificas desta rotina.
func adminSpentTokens(reader *bufio.Reader, gateways map[string]string) {
	fmt.Println("\n--- TOKENS GASTOS POR EMPRESA ---")
	companyID := selectCompany(reader, gateways)
	if companyID == "" {
		return
	}
	conn, _, err := getAvailableGatewayConn(gateways)
	if err != nil {
		fmt.Println("\n[ERRO] Malha offline.")
		reader.ReadString('\n')
		return
	}
	defer conn.Close()

	msg := Message{Type: "SPENT_TOKENS_REQ", CompanyID: companyID}
	json.NewEncoder(conn).Encode(msg)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var reply Message
	if err := json.NewDecoder(conn).Decode(&reply); err == nil && reply.Type == "SPENT_TOKENS_REP" {
		fmt.Println("\n=======================================================")
		fmt.Printf(" AUDITORIA DE TOKENS DA EMPRESA: %s\n", companyID)
		fmt.Println("=======================================================")
		fmt.Printf("- Tokens Ativos na carteira: %s (%s créditos)\n", reply.Payload["active_count"], reply.Payload["active_credits"])
		fmt.Printf("- Tokens Gastos (transferidos ao admin): %s (%s créditos)\n", reply.Payload["spent_count"], reply.Payload["spent_credits"])
	} else {
		fmt.Println("\n[ERRO] Falha ao consultar tokens gastos.")
	}
	fmt.Println("\nPressione Enter para voltar...")
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
