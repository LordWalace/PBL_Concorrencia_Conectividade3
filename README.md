# Sistema HORMUZ

Um sistema distribuido de coordenação de drones para quatro setores (Norte, Sul, Leste, Oeste) com foco em tolerância a falhas, exclusão mútua distribuída e redundância de rede.

## Sumario

- [Visao Geral](#visao-geral)
- [Arquitetura](#arquitetura)
- [Pre-requisitos](#pre-requisitos)
- [Configuracao](#configuracao)
- [Deploy Local](#deploy-local)
- [Deploy Distribuido](#deploy-distribuido)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Troubleshooting](#troubleshooting)

---

## Visao Geral

Este projeto implementa uma malha distribuida sem leader, sem ponto unico de falha e com:

- Exclusao mutua distribuida via algoritmo de Ricart-Agrawala
- Ordenacao com relogios logicos de Lamport
- Fila de alertas priorizada sem persistencia local
- Ledger Distribuido para controle de economia de tokens e prestacao de servicos
- Heartbeats e retries para deteccao de peers offline
- Fallback de beacons e migracao de devices quando gateways falham

### Componentes

- **Gateway** — coordena alertas, replicas de fila, dispatch de drones e mantem o ledger distribuido local.
- **Beacon** — gera alertas autonomos e faz failover para gateways ativos.
- **Device** — registra-se, envia heartbeats e executa comandos de despacho.
- **Client** — interface interativa para injetar alertas, contratar serviços e consultar status/saldo.
- **Admin** — painel de administracao do consorcio, emissao de tokens e auditoria global do ledger.

---

## Arquitetura

```text
┌───────────────────────────────────────────────────────────────────────┐
│                            GATEWAY                                    │
│  TCP: GATEWAY_TCP_REG_PORT   (registro, heartbeat, RA, alertas)       │
│  TCP: GATEWAY_TCP_CLIENT_PORT (cliente/admin -> alertas, saldo, etc)  │
│  TCP: GATEWAY_TCP_PEER_PORT   (P2P entre gateways, sincronia ledger)  │
└───────────────────────────────────────────────────────────────────────┘
           ▲                 ▲                 ▲                 ▲
           │                 │                 │                 │
┌──────────┴─────────┐ ┌─────┴──────┐ ┌────────┴────────┐ ┌──────┴─────┐
│      BEACON        │ │   CLIENT   │ │      ADMIN      │ │   DEVICE   │
│  entrega alertas   │ │ injeta req │ │ emite tokens e  │ │   recebe   │
│  com failover TCP  │ │  e saldo   │ │  audita ledger  │ │  dispatch  │
└────────────────────┘ └────────────┘ └─────────────────┘ └────────────┘
```

### Protocolo e Portas

| Componente |          Variavel          | Protocolo |                               Funcao                               |
|------------|----------------------------|-----------|--------------------------------------------------------------------|
|  Gateway   | `GATEWAY_TCP_REG_PORT`     |    TCP    | Registro de devices, heartbeat de drones, RA e ingestao de alertas |
|  Gateway   | `GATEWAY_TCP_CLIENT_PORT`  |    TCP    | Alertas manuais, status e log de eventos                           |
|  Gateway   | `GATEWAY_TCP_PEER_PORT`    |    TCP    | Mensagens P2P entre gateways e healthcheck                         |
   Device    | `DEVICE_CONTROL_PORT`      |    TCP    | Recebimento de comando DISPATCH                                    |

> Observacao: todas as conexoes entre componentes usam TCP. As referencias a UDP foram atualizadas para refletir a implementacao atual.

---

## Pre-requisitos

- Docker 20.10+
- Docker Compose 2.0+
- Go 1.22+ (somente para compilacao local, opcional para execucao em containers)

---

## Configuracao

### .env e variaveis esperadas

Antes de iniciar qualquer serviço, crie o arquivo `.env` local a partir do modelo `.env.example`.
O arquivo `.env` deve ser gerado apenas uma vez pelo seu ambiente local, usando as informacoes comentadas em `.env.example`.

> Importante: `.env.example` contem blocos de exemplo para todos os setores, clientes e administrador.
> Em um ambiente distribuido, copie apenas o bloco relevante para o seu host e nao todo o arquivo.

Se voce estiver testando tudo em um unico PC com o compose raiz, é possivel começar com:

```bash
copy .env.example .env
```

mas em geral, para deploy distribuido, abra `.env.example` e copie somente o bloco correto para o seu setor ou cliente.

O `docker-compose.yml` principal exige nomes de variaveis por setor como `GATEWAY_ID_NORTE`, `GATEWAY_IP_NORTE`, `DEVICE_ID_NORTE`, `DEVICE_IP_NORTE`, `SETOR_ID_NORTE`, entre outros.

Variaveis globais importantes:

- `IP_NORTE`, `IP_SUL`, `IP_LESTE`, `IP_OESTE`
- `GATEWAY_TCP_CLIENT_PORT`
- `GATEWAY_TCP_REG_PORT`
- `GATEWAY_TCP_PEER_PORT`
- `MISSION_DURATION`
- `BEACON_MIN_INTERVAL`, `BEACON_MAX_INTERVAL`

Para cada setor, configure:

- `GATEWAY_ID_<SETOR>`
- `GATEWAY_IP_<SETOR>`
- `GATEWAY_HOST_<SETOR>=0.0.0.0`
- `SETOR_ID_<SETOR>`
- `DEVICE_ID_<SETOR>`
- `DEVICE_IP_<SETOR>`
- `DEVICE_HOST_<SETOR>=0.0.0.0`
- `DEVICE_CONTROL_PORT_<SETOR>`

---

## Deploy Local (modo integracao)

1. Construa as imagens:

```bash
docker compose build
```

2. Suba um setor completo:

```bash
docker compose up -d gateway-norte beacon-norte drone-norte
```

3. (Opcional) suba o cliente interativo:

```bash
docker compose up -d client
```

4. Pare a malha:

```bash
docker compose down
```

---

## Deploy Distribuido (host mode)

Para executar cada componente em um host separado, use os scripts de inicialização fornecidos em `scripts/`.
Eles ja tratam de subir os serviços corretos a partir das pastas correspondentes e usam os arquivos `docker-compose.yml` locais de cada componente.

Para deploy em uma unica maquina de teste, use o arquivo `docker-compose.yml` raiz do projeto.
Esse root compose é o caminho recomendado quando todos os serviços precisam rodar no mesmo PC.

---

## Uso

- Inicie todos os gateways antes de subir devices e beacons.
- O `client` injeta alertas manuais e valida `ALERT_ACK`.
- O `gateway` replica alertas, negocia exclusao mutua e dispara drones com `DISPATCH`.
- O `device` mantem heartbeat e migra para outro gateway se a conexao falhar.

### Consultar status

O `client` emite consultas de status e log para cada gateway do setor.

### Failsafe e tolerancia a falhas

- Beacons fazem failover entre gateways ativos.
- Gateways monitoram peers e marcam offline os que nao respondem.
- Dispositivos reenregistram e migram automatico quando o gateway atual falha.
- Filas de alertas sao persistidas localmente para evitar perda de dados em reinicios.

---

## Estrutura do Projeto

```text
PBL_Redes-Sensores2/
├── README.md
├── docker-compose.yml
├── .env
├── .env.example
├── gateway/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── main.go
├── beacon/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── main.go
├── device/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── main.go
├── client/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── main.go
└── admin/
    ├── Dockerfile
    ├── docker-compose.yml
    └── main.go
```

---

## Troubleshooting

### Variaveis de ambiente incorretas

Verifique se o `.env` contem os nomes usados pelo `docker-compose.yml` principal, incluindo sufixos de setor (`_NORTE`, `_SUL`, `_LESTE`, `_OESTE`).

### Gateway desconectado

```bash
docker compose ps
docker compose logs gateway-norte
```

### Porta em uso

```bash
netstat -ano | findstr :8080
taskkill /PID <PID> /F
```

### Cliente mostrando setor OFFLINE

O cliente trata gateways offline de forma silenciosa: ele exibe `OFFLINE` quando nao ha resposta do setor.

### Logs adicionais

- Gateway: `docker compose logs gateway-norte`
- Beacon: `docker compose logs beacon-norte`
- Device: `docker compose logs drone-norte`
- Client: `docker compose logs client`

---

## Notas finais

- Cada `main.go` em `gateway/`, `device/`, `beacon/` e `client/` contem um bloco de declaracao academica de autoria.
- O `gateway` usa Ricart-Agrawala para exclusao mutua e Lamport para ordenacao de eventos.
- A arquitetura foi projetada para funcionar com IPs reais de rede e sem `localhost`.
