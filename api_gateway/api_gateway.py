import threading
import time
import json
import os
import Pyro5.api
import Pyro5.server
from kafka import KafkaConsumer

# --- Constantes de Configuração ---
KAFKA_TOPIC = "server_heartbeats"
KAFKA_SERVERS = ['localhost:9092']  # Mude para o endereço do seu broker Kafka
STATE_FILE = "gateway_state.json"
HEARTBEAT_TIMEOUT_SECONDS = 5 # Tempo para considerar um servidor morto (3x o intervalo de heartbeat do servidor)

@Pyro5.api.expose
class APIGateway:
    def __init__(self):
        """
        Inicializa o API Gateway.
        - Carrega o estado do disco, se existir.
        - Inicializa as estruturas de dados para roteamento e monitoramento.
        - Cria um Lock para garantir a segurança em ambiente com múltiplas threads.
        """
        print("Iniciando API Gateway...")
        # Tabela principal de roteamento: {"ServiceName": ["URI1", "URI2", ...]}
        self.service_routing_table = {}
        # Contadores para o balanceamento de carga Round-Robin
        self.round_robin_counter = 0
        # Rastreia o último heartbeat de cada servidor para detectar falhas
        self.server_last_heartbeat = {}
        
        # O Lock é ESSENCIAL para proteger o acesso às estruturas de dados acima,
        # já que elas serão acessadas pela thread do Kafka e pelas threads do Pyro.
        self.lock = threading.Lock()

        self.load_state_from_disk()
        print(f"Estado inicial carregado. Tabela de roteamento: {self.service_routing_table}")

    def load_state_from_disk(self):
        """Carrega a tabela de roteamento de um arquivo JSON para persistência."""
        with self.lock:
            if os.path.exists(STATE_FILE):
                try:
                    with open(STATE_FILE, 'r') as f:
                        state = json.load(f)
                        self.service_routing_table = state.get("routing_table", {})
                        self.round_robin_counters = state.get("rr_counter", int)
                        print("Estado recuperado do disco com sucesso.")
                except (json.JSONDecodeError, IOError) as e:
                    print(f"Erro ao ler o arquivo de estado: {e}. Começando com estado vazio.")

    def save_state_to_disk(self):
        """Salva o estado atual da tabela de roteamento em um arquivo JSON."""
        # Esta função deve ser chamada dentro de um 'with self.lock:'
        state = {
            "routing_table": self.service_routing_table,
            "rr_counter": self.round_robin_counter
        }
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, indent=4)
        except IOError as e:
            print(f"Erro crítico: não foi possível salvar o estado no disco: {e}")

    def _heartbeat_callback(self, kafka_message):
        """
        Processa uma mensagem de heartbeat recebida do Kafka.
        Esta função é o "cérebro" da descoberta de serviço.
        """
        try:
            heartbeat_data = json.loads(kafka_message)
            daemon_location = heartbeat_data["daemon_location"]
            hosted_services = heartbeat_data["hostedServices"]
            
            with self.lock:
                print(f"Heartbeat recebido de: {daemon_location} hospedando {hosted_services}")
                
                # Atualiza o timestamp do último heartbeat
                self.server_last_heartbeat[daemon_location] = time.time()
                
                # Lógica de "upsert": Adiciona ou atualiza os serviços na tabela de roteamento
                for service_name in hosted_services:
                    # Garante que a lista para o serviço exista
                    if service_name not in self.service_routing_table:
                        self.service_routing_table[service_name] = []
                    
                    # Adiciona a URI do servidor à lista do serviço, se ainda não estiver lá
                    service_uri = f"PYRO:{service_name}@{daemon_location}"
                    if service_uri not in self.service_routing_table[service_name]:
                        self.service_routing_table[service_name].append(service_uri)
                        print(f"Novo objeto remoto {service_uri} registrado para o serviço {service_name}.")
                
                self.save_state_to_disk()

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Erro ao processar mensagem do Kafka: {e} - Mensagem: {kafka_message}")

    def _reaper_thread_func(self):
        """
        Thread "Ceifadora" que remove servidores mortos (que não enviam heartbeat).
        """
        print("Thread Reaper iniciada. Verificando servidores mortos...")
        while True:
            time.sleep(HEARTBEAT_TIMEOUT_SECONDS / 2) # Verifica na metade do tempo do timeout
            
            with self.lock:
                now = time.time()
                dead_servers = [
                    server_uri for server_uri, last_beat in self.server_last_heartbeat.items()
                    if now - last_beat > HEARTBEAT_TIMEOUT_SECONDS
                ]
                
                if dead_servers:
                    print(f"Servidores mortos detectados: {dead_servers}. Removendo...")
                    for server_uri in dead_servers:
                        # Remove dos registros de heartbeat
                        del self.server_last_heartbeat[server_uri]
                        
                        # Remove de todas as listas de serviços na tabela de roteamento
                        for service_name in list(self.service_routing_table.keys()):
                            service_uri = f"PYRO:{service_name}@{server_uri}"
                            if service_uri in self.service_routing_table[service_name]:
                                self.service_routing_table[service_name].remove(service_uri)
                                print(f"Servidor {server_uri} removido do serviço {service_name}.")
                            
                            # Se um serviço ficar sem servidores, remove a entrada do serviço
                            if not self.service_routing_table[service_name]:
                                del self.service_routing_table[service_name]
                    
                    self.save_state_to_disk()

    @Pyro5.api.oneway
    def log_message(self, message):
        """Um método simples para teste de chamada oneway."""
        print(f"MENSAGEM DE LOG RECEBIDA: {message}")

    @Pyro5.api.expose
    def forward_request(self, service_name, method_name, *args, **kwargs):
        """
        O principal método do Gateway. Recebe uma requisição e a encaminha
        para o serviço de back-end apropriado usando balanceamento de carga.
        """
        print(f"Requisição recebida para: {service_name}.{method_name}")
        
        chosen_uri = None
        with self.lock:
            # Verifica se o serviço existe e tem servidores disponíveis
            if service_name not in self.service_routing_table or not self.service_routing_table[service_name]:
                print(f"ERRO: Nenhum servidor disponível para o serviço '{service_name}'.")
                raise ValueError(f"Serviço '{service_name}' não encontrado ou indisponível.")

            # Lógica de Balanceamento de Carga - Round Robin
            service_list = self.service_routing_table[service_name]
            index = self.round_robin_counter
            chosen_uri = service_list[index]
            
            # Atualiza o contador para a próxima requisição
            self.round_robin_counter = (index + 1) % len(service_list)
        
        print(f"Encaminhando para: {chosen_uri}")
        
        try:
            # Cria o proxy para o servidor escolhido sob demanda
            with Pyro5.api.Proxy(chosen_uri) as backend_proxy:
                # Pega o método desejado do objeto remoto
                remote_method = getattr(backend_proxy, method_name)
                # Invoca o método e retorna o resultado
                return remote_method(*args, **kwargs)
        except Exception as e:
            print(f"ERRO CRÍTICO ao se comunicar com o servidor de back-end {chosen_uri}: {e}")
            # Em uma implementação mais avançada, poderia haver uma nova tentativa
            # ou a remoção imediata do servidor problemático.
            raise ConnectionError(f"Falha ao contatar o serviço de back-end em {chosen_uri}")


def kafka_consumer_thread_func(gateway: APIGateway):
    """Thread que consome o tópico Kafka e chama o callback do gateway."""
    print("Thread Kafka Consumer iniciada.")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='latest', # Começa a ler as mensagens mais recentes
            group_id='api-gateway-group' # Permite escalar o gateway no futuro
        )
        for message in consumer:
            gateway._heartbeat_callback(message.value.decode('utf-8'))
    except Exception as e:
        print(f"Erro fatal na thread do Kafka Consumer: {e}. A thread será encerrada.")

if __name__ == "__main__":
    # 1. Instanciar o Gateway
    gateway = APIGateway()
    
    # 2. Configurar e iniciar a thread do consumer Kafka
    kafka_thread = threading.Thread(target=kafka_consumer_thread_func, args=(gateway,))
    kafka_thread.daemon = True
    kafka_thread.start()
    
    # 3. Configurar e iniciar a thread "Reaper"
    reaper_thread = threading.Thread(target=gateway._reaper_thread_func)
    reaper_thread.daemon = True
    reaper_thread.start()

    # 4. Configurar e iniciar o daemon do Pyro
    # PONTO CRÍTICO: Usar 'pooled' para paralelismo real.
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(gateway, objectId="APIGateway")
    ns = Pyro5.api.locate_ns()  # Localiza o Name Server Pyro
    ns.register("APIGateway", uri)  # Registra o Gateway no Name Server
    
    print("="*50)
    print(f"API Gateway pronto e ouvindo em: {uri}")
    print("="*50)
    
    # 5. Iniciar o loop principal do Pyro (bloqueante)
    daemon.requestLoop()