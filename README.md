
# BigFS - Sistema Distribuído de Armazenamento de Arquivos

Este projeto é um sistema de armazenamento distribuído desenvolvido como parte da Entrega 2 da disciplina de Sistemas Distribuídos. Ele implementa um serviço chamado **BigFS**, capaz de distribuir, replicar e gerenciar arquivos entre diferentes máquinas (workers), com um servidor mestre central e comunicação via Kafka e Pyro5.

---

## Requisitos

Antes de executar o projeto, certifique-se de ter os seguintes requisitos instalados:

- Python 3.8 ou superior
- Kafka + Zookeeper (executado via Docker Compose)
- Biblioteca Pyro5
- Biblioteca psutil
- Biblioteca kafka-python
- Docker e Docker Compose

---

## Instalação de dependências Python

Se ainda não tiver as bibliotecas Python necessárias, instale com:

```
pip install Pyro5 kafka-python psutil
```

---

## Passos para executar o sistema

### 1. Subindo o Kafka e o Zookeeper com Docker Compose

É necessário que o Kafka esteja rodando para que o servidor mestre e os workers possam se comunicar.

Execute:

```
docker compose up kafka
```

Esse comando irá iniciar tanto o **Kafka** quanto o **Zookeeper**.

---

### 2. Subindo o Name Server do Pyro5

O Name Server permite a descoberta dinâmica de objetos distribuídos.

Execute:

```
python3 -m Pyro5.nameserver -n 0.0.0.0
```

> Este comando expõe o Name Server em todas as interfaces de rede, permitindo que workers e clientes em outras máquinas o encontrem.

---

### 3. Executando o Servidor Mestre (server_master.py)

O servidor mestre é o núcleo do sistema, responsável por:

- Monitorar o status dos workers via Kafka.
- Gerenciar o mapeamento de arquivos e balanceamento de carga.
- Responder às requisições do cliente.

No diretório raiz do projeto, execute:

```
python3 server_master.py
```

---

### 4. Executando os Workers (worker.py)

Os workers são responsáveis por armazenar fisicamente os arquivos.

Eles podem ser executados **em máquinas diferentes**, desde que todas consigam se comunicar com o Kafka e com o Name Server.

Para iniciar um worker:

```
python3 worker.py
```

Você pode rodar múltiplos workers (cada um com um ID único gerado automaticamente).

---

### 5. Executando o Cliente (client.py)

O cliente é responsável por fazer upload, download e listar arquivos no sistema distribuído.

Em qualquer máquina que tenha acesso ao Name Server e ao servidor mestre, execute:

```
python3 client.py
```

---

## Observações importantes

- Certifique-se de que o **Kafka**, o **Zookeeper** e o **Name Server** estejam ativos antes de iniciar o servidor e os workers.

- Workers e servidor mestre devem ter acesso de rede ao broker Kafka e ao Name Server.

- Em ambientes com múltiplas máquinas, ajuste o IP de conexão com o Kafka e o Pyro5 conforme necessário.

---

## Estrutura básica do projeto

```
.
├── server_master.py
├── worker.py
├── client.py
├── docker-compose.yml
├── README.md
└── ...
```

---
