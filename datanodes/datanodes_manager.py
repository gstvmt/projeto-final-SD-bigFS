# datanodes_manager.py

from kafka import KafkaConsumer, KafkaProducer
import Pyro5.server
import threading
import Pyro5.api
import time
import json

# --- Constantes de Configuração ---
HEARTBEAT_TOPIC = "datanode_heartbeats"
CLUSTER_UPDATE_TOPIC = "datanode_cluster_updates"
KAFKA_SERVERS = ['localhost:9092']
NODE_TIMEOUT_SECONDS = 5                           # Considera um nó morto se não houver heartbeat por este tempo
# ----------------------------------

@Pyro5.api.expose
class DataNodesManager:
    """
    Gerencia o estado dos DataNodes. Atua como um consumidor Kafka para heartbeats,
    um produtor Kafka para atualizações de cluster e um servidor Pyro para
    fornecer informacoes de todos os nos ativos no momento.
    """

    def __init__(self):
        self.datanodes = {} 
        self.lock = threading.Lock()
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"ERRO CRÍTICO ao conectar ao Kafka Producer: {e}. O Manager não poderá publicar atualizações.")
            self.producer = None
        print("DataNodes Manager inicializado.")

#================================= Metodos ===========================================

    def publish_update(self, event_type: str, node_uri: str):
        """
        Publica uma mudança no estado do cluster para os servidores.
        """
        if not self.producer:
            print("AVISO: Kafka Producer não está disponível. Pulando publicação de atualização.")
            return
            
        message = {"eventType": event_type, "node_uri": node_uri, "timestamp": time.time()}
        print(f"Publicando atualização: {message}")
        self.producer.send(CLUSTER_UPDATE_TOPIC, message)

    def _heartbeat_callback(self, kafka_message: bytes):
        """
        Processa um heartbeat de um DataNode.
        """
        try:
            print("ping")
            data = json.loads(kafka_message)
            node_uri = data["address"]
            memory_used_percent = data.get("memory_used_percent", 0)
            disk_free_gb = data.get("disk_free_gb", 0)
            disk_read_rate_bps = data.get("disk_read_rate_bps", 0)
            disk_write_rate_bps = data.get("disk_write_rate_bps", 0)
            
            with self.lock:
                # Verifica se é um nó novo ou um nó que estava DOWN e voltou
                is_new_or_resurrected = node_uri not in self.datanodes or self.datanodes[node_uri]["status"] == "DOWN"
                
                self.datanodes[node_uri] = {"last_seen": time.time(), "status": "UP", 
                                            "memory_used_percent": memory_used_percent,
                                            "disk_free_gb": disk_free_gb,
                                            "disk_read_rate_bps": disk_read_rate_bps,
                                            "disk_write_rate_bps": disk_write_rate_bps}
                
                if is_new_or_resurrected:
                    print(f"DataNode UP detectado: {node_uri}")
                    self.publish_update("NODE_UP", node_uri)
                    
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Erro ao processar heartbeat: {e}")

    def _reaper_thread_func(self):
        """
        Thread que remove nós que não enviam heartbeats.
        """
        print("Thread Reaper de DataNodes iniciada.")

        while True:
            time.sleep(NODE_TIMEOUT_SECONDS / 2)
            
            with self.lock:
                now = time.time()
                timed_out_nodes = [
                    uri for uri, status_info in self.datanodes.items()
                    if now - status_info["last_seen"] > NODE_TIMEOUT_SECONDS and status_info["status"] == "UP"
                ]
                
                for node_uri in timed_out_nodes:
                    print(f"DataNode timed out: {node_uri}. Marcando como DOWN.")
                    self.datanodes[node_uri]["status"] = "DOWN"
                    self.publish_update("NODE_DOWN", node_uri)

    @Pyro5.api.expose
    def get_active_datanodes(self) -> list:
        """
        Método RMI exposto para bootstrap. Retorna a lista de URIs de nós ativos.
        """
        print("[RMI Call] Recebida requisição para obter nós ativos...")

        with self.lock:
            active_nodes = [
                uri for uri, status_info in self.datanodes.items()
                if status_info["status"] == "UP"
            ]
        print(f"[RMI Call] Retornando {len(active_nodes)} nós ativos.")
        return active_nodes

#================================= Thread Consumer kafka ===========================================

def kafka_consumer_thread_func(manager: DataNodesManager):
    """
    Consome o tópico de heartbeats e chama o callback.
    """
    print("Thread consumidora de heartbeats de DataNode iniciada.")

    try:
        consumer = KafkaConsumer(
            HEARTBEAT_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='latest'
        )
        for message in consumer:
            manager._heartbeat_callback(message.value)
    except Exception as e:
        print(f"Erro fatal na thread do Kafka Consumer: {e}. A thread será encerrada.")

#======================================== Main ================================================

if __name__ == "__main__":
    
    # instancia um manager
    manager = DataNodesManager()
    
    # inicia as threads de background (Kafka consumer e Reaper)
    consumer_thread = threading.Thread(target=kafka_consumer_thread_func, args=(manager,))
    consumer_thread.daemon = True
    consumer_thread.start()
    
    reaper_thread = threading.Thread(target=manager._reaper_thread_func)
    reaper_thread.daemon = True
    reaper_thread.start()
    
    # inicia o daemon pyro para expor o manager como um serviço
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(manager, objectId="DataNodesManager")
    ns = Pyro5.api.locate_ns()  # Localiza o Name Server Pyro
    ns.register("DataNodesManager", uri)  # Registra o Manager no Name Server
    
    print("="*50)
    print(" DataNodes Manager está rodando.")
    print(f"   Serviço RMI disponível em: {uri}")
    print("="*50)
    
    # loop principal
    daemon.requestLoop()