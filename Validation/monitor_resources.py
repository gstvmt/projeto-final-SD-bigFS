from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import threading
import time

class KafkaResourceMonitor:
    def __init__(self, kafka_servers=['localhost:9092'], topic="datanode_heartbeats"):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.monitoring = False
        self.data = {}  # {node_id: {'memory': [], 'disk': [], 'read_rate': [], 'write_rate': [], 'timestamps': []}}
        self.consumer = None
        self.thread = None
        
    def start(self):
        self.monitoring = True
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()
        
    def stop(self):
        self.monitoring = False
        if self.consumer:
            self.consumer.close()
        if self.thread:
            self.thread.join()
        
    def _monitor(self):
        start_time = time.time()
        for message in self.consumer:
            if not self.monitoring:
                break
                
            payload = message.value
            node_id = payload['node_name']
            
            # Inicializa estrutura para novo nó
            if node_id not in self.data:
                self.data[node_id] = {
                    'memory': [],
                    'disk': [],
                    'read_rate': [],
                    'write_rate': [],
                    'timestamps': []
                }
            
            # Adiciona dados ao nó específico
            timestamp = time.time() - start_time
            self.data[node_id]['timestamps'].append(timestamp)
            self.data[node_id]['memory'].append(payload['memory_used_percent'])
            self.data[node_id]['disk'].append(payload['disk_free_gb'])
            self.data[node_id]['read_rate'].append(payload['disk_read_rate_bps'] / (1024 * 1024))
            self.data[node_id]['write_rate'].append(payload['disk_write_rate_bps'] / (1024 * 1024))
    
    def show_results(self):
        if not self.data:
            print("Nenhum dado foi coletado.")
            return
            
        for node_id, node_data in self.data.items():
            if not node_data['timestamps']:
                continue
                
            print(f"\n=== Resultados para o nó: {node_id} ===")
            
            # Cria figura com subplots
            plt.figure(figsize=(15, 10))
            plt.suptitle(f"Monitoramento de Recursos - Nó {node_id}")
            
            # Plot Memória
            plt.subplot(2, 2, 1)
            plt.plot(node_data['timestamps'], node_data['memory'], 'r-')
            plt.title('Uso de Memória (%)')
            plt.xlabel('Tempo (s)')
            plt.ylabel('Percentual')
            plt.grid(True)
            
            # Plot Disco Livre
            plt.subplot(2, 2, 2)
            plt.plot(node_data['timestamps'], node_data['disk'], 'b-')
            plt.title('Espaço Livre em Disco (GB)')
            plt.xlabel('Tempo (s)')
            plt.ylabel('GB')
            plt.grid(True)
            
            # Plot Taxa de Leitura
            plt.subplot(2, 2, 3)
            plt.plot(node_data['timestamps'], node_data['read_rate'], 'g-')
            plt.title('Taxa de Leitura de Disco (MB/s)')
            plt.xlabel('Tempo (s)')
            plt.ylabel('Bytes por segundo')
            plt.grid(True)
            
            # Plot Taxa de Escrita
            plt.subplot(2, 2, 4)
            plt.plot(node_data['timestamps'], node_data['write_rate'], 'm-')
            plt.title('Taxa de Escrita em Disco (MB/s)')
            plt.xlabel('Tempo (s)')
            plt.ylabel('Bytes por segundo')
            plt.grid(True)
            
            plt.tight_layout()
            plt.show()
            
            # Estatísticas resumidas
            print("\nEstatísticas Resumidas:")
            print(f"Memória (%): Média = {sum(node_data['memory'])/len(node_data['memory']):.2f}")
            print(f"Disco Livre (GB): Média = {sum(node_data['disk'])/len(node_data['disk']):.2f}")
            print(f"Taxa de Leitura (MB/s): Média = {sum(node_data['read_rate'])/len(node_data['read_rate']):.2f}")
            print(f"Taxa de Escrita (MB/s): Média = {sum(node_data['write_rate'])/len(node_data['write_rate']):.2f}")


# Exemplo de uso:
if __name__ == "__main__":
    monitor = KafkaResourceMonitor()
    monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        monitor.stop()
        monitor.show_results()