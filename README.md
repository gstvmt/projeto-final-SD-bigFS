
# BigFS - Sistema Distribuído de Armazenamento de Arquivos

Este projeto é um sistema de armazenamento distribuído desenvolvido como entrega final da disciplina de Sistemas Distribuídos. Ele implementa um serviço capaz de distribuir, replicar e gerenciar arquivos entre diferentes nós, com servidores e comunicação via Kafka e Pyro5.

---

## Requisitos

Antes de executar o projeto, certifique-se de ter os seguintes requisitos instalados:

- Python 3.8 ou superior
- Kafka + Zookeeper (será executado via Docker Compose)
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

É necessário que o Kafka esteja rodando para que o sistema possa se comunicar, compartilhando informações como heartbeat e inconsistencia nos metadados.

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

> Este comando expõe o Name Server em todas as interfaces de rede, permitindo que, por exemplo, servidores e clientes em outras máquinas o encontrem.

---

### 3. Executando o Servidor de Metadados (metadata_server.py)

Nesta etapa, é necessário instanciar o servidor de metadados, que será o ponto central para gerenciar as informações sobre os arquivos e sua localização no sistema distribuído.

```
python3 metadata/metadata_server.py
```

---

### 4. Iniciando o Gerenciador de Nós (datanodes_manager.py)

O gerenciador de nós é responsável por supervisionar a disponibilidade dos diferentes nós de armazenamento no sistema, garantindo a replicação e a consistência dos dados.

```
python3 datanodes/datanodes_manager.py
```

---

### 5. Iniciando a API (api_gateway.py)

Por fim, inicie a API do sistema. Esta interface permitirá a interação externa com o BigFS, possibilitando que clientes enviem requisições para armazenar, recuperar e gerenciar arquivos.

```
python3 api_gateway/api_gateway.py
```

---

### 6. Executando o Servidor (server.py)

O servidor é o núcleo do sistema, responsável por:

- Gerenciar o mapeamento de arquivos e balanceamento de carga.
- Responder às requisições do cliente.

No diretório raiz do projeto, execute:

```
python3 server/server.py
```

---

### 7. Executando os Nós (datanode.py)

Os nós são responsáveis por armazenar fisicamente os arquivos.

Eles podem ser executados **em máquinas diferentes**, desde que todas consigam se comunicar com o Kafka e com o Name Server e, como temos replica 2, é necessário instanciar pelo manos 2 nós.

Para iniciar um worker:

```
python3 datanodes/datanode.py
```

Você pode rodar múltiplos nó (cada um com um ID único gerado automaticamente).

---

### 8. Executando o Cliente (client.py)

O cliente é responsável por fazer upload, download e listar arquivos no sistema distribuído. Execute:

```
python3 client/client.py
```

**Épossível também rodar os escripts de banchmark que estão no diretório Validation**

---

## Observações importantes

- Certifique-se de que o **Kafka**, o **Zookeeper** e o **Name Server** estejam ativos antes de iniciar o servidor e os workers.

- Em ambientes com múltiplas máquinas, ajuste o IP de conexão com o Kafka e o Pyro5 conforme necessário.

---