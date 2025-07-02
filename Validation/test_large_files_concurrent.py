# Cenário 6
import threading
import time
import os
from queue import Queue
import Pyro5.api
from utils import generate_random_file, drop_caches
from monitor_resources import KafkaResourceMonitor
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../client')))
from client import FileSystemClient

class VirtualUser(threading.Thread):
    def __init__(self, user_id, file_queue):
        super().__init__()
        self.user_id = user_id
        self.file_queue = file_queue

    def run(self):
        nameserver = Pyro5.api.locate_ns()
        client = FileSystemClient(nameserver, f"{self.user_id}")
        while True:
            file_info = self.file_queue.get()
            if file_info is None:
                self.file_queue.task_done()
                break
                
            filepath = file_info
            filename = os.path.basename(filepath)
            
            try:
                remote_path = f"dfs:/teste_{self.user_id}"
                client.upload_file(filepath, remote_path)
                        
            except Exception as e:
                print(f"Erro no usuário {self.user_id} ao processar {filename}: {e}")
                
            self.file_queue.task_done()

def test_concurrent_large_transfers():
    # Configuração do teste
    num_users = 5
    files_per_user = 1  # 5 usuários x 1 arquivo = 5 transferências concorrentes
    file_size_gb = 4
    test_dir = "test_concurrent_large"
    
    # Preparar ambiente
    os.makedirs(test_dir, exist_ok=True)
    
    # Criar arquivos grandes (isso pode demorar muito!)
    print(f"Criando {num_users * files_per_user} arquivos de {file_size_gb}GB cada...")
    user_files = {}
    for user_id in range(num_users):
        user_files[user_id] = []
        for file_num in range(files_per_user):
            filename = f"large_file_user_{user_id}_{file_num}.txt"
            filepath = os.path.join(test_dir, filename)
            
            if not os.path.exists(filepath):
                print(f"Criando {filename}...")
                generate_random_file(filepath, file_size_gb * 1024 * 1024)  # GB
                
            user_files[user_id].append(filepath)
    
    drop_caches()

    # Preparar estruturas para o teste
    file_queue = Queue()
    
    # Adicionar tarefas (upload para cada arquivo)
    for user_id, files in user_files.items():
        for filepath in files:
            file_queue.put(filepath)
    
    # Iniciar monitoramento de recursos
    monitor = KafkaResourceMonitor()
    print("Iniciando monitoramento de recursos...")
    monitor.start()

    users = []
    for user_id in range(num_users):
        user = VirtualUser(user_id, file_queue)
        user.start()
        users.append(user)
    
    # Esperar todas as tarefas serem processadas
    file_queue.join()
    
    # Encerrar usuários
    for _ in range(num_users):
        file_queue.put(None)
    for user in users:
        user.join()
    
    # Parar monitoramento
    monitor.stop()
    
    monitor.show_results()

if __name__ == "__main__":
    test_concurrent_large_transfers()