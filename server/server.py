import threading
import time
import json
import Pyro5.api
import Pyro5.server
from kafka import KafkaConsumer, KafkaProducer

# Importando os componentes e serviços
from shared.metadata_repo import MetadataRepository
from services.datanodes_registry import DataNodeRegistry
from services.list_service import ListService
from services.remove_service import RemoveService
from services.copy_service import CopyService 

# --- Configuração ---
HEARTBEAT_INTERVAL_SECONDS = 5
KAFKA_HEARTBEAT_TOPIC = "server_heartbeats" # Heartbeat deste próprio servidor
KAFKA_UPDATE_TOPIC = "datanode_cluster_updates" # Tópico para ouvir o Manager
KAFKA_INVALIDATION_TOPIC = "metadata_invalidation_events"
KAFKA_SERVERS = ['localhost:9092']


# --- Threads de Background ---


def server_heartbeat_producer(daemon_uri, service_names, kafka_producer):

    """Envia o heartbeat deste servidor de lógica para o API Gateway."""

    payload = {"daemon_location": str(daemon_uri), "hostedServices": service_names}
    print(f"Thread de Heartbeat do Servidor iniciada para {daemon_uri}.")

    while True:
        payload["timestamp"] = time.time()
        kafka_producer.send(KAFKA_HEARTBEAT_TOPIC, payload)
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def datanode_update_consumer(registry: DataNodeRegistry):

    """Consome o tópico de atualizações do cluster de DataNodes."""

    print("Thread consumidora de atualizações de DataNode iniciada.")
    try:
        consumer = KafkaConsumer(
            KAFKA_UPDATE_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='latest'
            # Não usamos group_id aqui para receber todas as mensagens
            )
        for message in consumer:
            try:
                update = json.loads(message.value.decode('utf-8'))
                event_type = update["eventType"]
                node_uri = update["node_uri"]
                
                if event_type == "NODE_UP":
                    registry.add_node(node_uri)
                elif event_type == "NODE_DOWN":
                    registry.remove_node(node_uri)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Erro ao parsear mensagem de atualização do cluster: {e}")
    except Exception as e:
        print(f"Erro fatal na thread consumidora de atualizações: {e}.")


def cache_invalidation_consumer(repo: MetadataRepository):
    """
    Consome o tópico de invalidação de metadados e chama o callback
    no MetadataRepository para limpar o cache local.
    """
    print("Thread consumidora de invalidação de cache iniciada.")
    try:
        consumer = KafkaConsumer(
            KAFKA_INVALIDATION_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset='latest'
        )
        for message in consumer:
            try:
                invalidation_data = json.loads(message.value.decode('utf-8'))
                path_to_invalidate = invalidation_data.get("path")
                
                if path_to_invalidate:
                    # Este é o callback: chama o método do repositório
                    repo.invalidate_cache(path_to_invalidate)

            except Exception as e:
                print(f"Erro ao processar mensagem de invalidação de cache: {e}")
    except Exception as e:
        print(f"Erro fatal na thread de invalidação de cache: {e}.")

# ---------------------------------------------------------------------

if __name__ == "__main__":

    # REFERENCIAS REMOTAS ESSENCIAIS

    ns = Pyro5.api.locate_ns()  # Localiza o Name Server Pyro
    try:
        manager_uri = ns.lookup("DataNodesManager")
        assert str(manager_uri).startswith("PYRO:"), "URI inválida."
    except (AssertionError, EOFError):
        print("URI do manager é necessária e deve ser válida. Encerrando.")
        exit(1)

    try:
        metadata_service_uri = ns.lookup("MetadataService")
        assert str(metadata_service_uri).startswith("PYRO:"), "URI inválida."
    except (AssertionError, EOFError):
        print("URI do MetadataService é necessária e deve ser válida. Encerrando.")
        exit(1)


    # COMPONENTES COMPARTILHADOS

    print("Inicializando componentes compartilhados...")
    datanode_registry = DataNodeRegistry()
    metadata_repo = MetadataRepository(metadata_service_uri)
    
    
    # LÓGICA DE BOOTSTRAP - BUSCAR ESTADO INICIAL
    try:
        print(f"Realizando bootstrap com o DataNodesManager em {manager_uri}...")
        with Pyro5.api.Proxy(manager_uri) as manager_proxy:
            initial_active_nodes = manager_proxy.get_active_datanodes()
        
        datanode_registry.bootstrap(initial_active_nodes)
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao realizar bootstrap: {e}. O servidor pode operar sem a lista completa de DataNodes.")

   
    # INSTANCIAR O DAEMON PYRO  
    daemon = Pyro5.server.Daemon()

    # INSTANCIAR E REGISTRAR OS SERVIÇOS
    print("Instanciando e registrando serviços...")
    list_svc = ListService(metadata_repo)
    remove_svc = RemoveService(metadata_repo) # Este serviço também precisaria do DataNodeClient
    copy_svc = CopyService(daemon, metadata_repo, datanode_registry)
    
    daemon.register(list_svc, objectId="ListService")
    daemon.register(remove_svc, objectId="RemoveService")
    daemon.register(copy_svc, objectId="CopyService")
    
    service_names = list(daemon.objectsById.keys())
    uri_para_anunciar = str(daemon.uriFor(list_svc))
    uri_para_anunciar = uri_para_anunciar.split('@')[1] 

    # INICIAR THREADS DE BACKGROUND
    try:
        # Thread para o heartbeat deste próprio servidor
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        server_heartbeat_thread = threading.Thread(
            target=server_heartbeat_producer,
            args=(uri_para_anunciar, service_names, kafka_producer)
        )
        server_heartbeat_thread.daemon = True
        server_heartbeat_thread.start()

        # Thread para ouvir atualizações sobre os DataNodes
        update_consumer_thread = threading.Thread(
            target=datanode_update_consumer, 
            args=(datanode_registry,)
            )
        update_consumer_thread.daemon = True
        update_consumer_thread.start()

        invalidation_thread = threading.Thread(
            target=cache_invalidation_consumer, 
            args=(metadata_repo,) # Passa a instância do repositório para o callback
        )
        invalidation_thread.daemon = True
        invalidation_thread.start()

    except Exception as e:
        print(f"ERRO CRÍTICO ao iniciar threads de background com o Kafka: {e}")

    # INICIAR O LOOP DO SERVIDOR
    print("="*50)
    print(f" Servidor de Back-end pronto e ouvindo em: {uri_para_anunciar}")
    print(f"   Serviços hospedados: {service_names}")
    print(f"   Nós de dados conhecidos após bootstrap: {len(datanode_registry.get_all())}")
    print("="*50)
    daemon.requestLoop()