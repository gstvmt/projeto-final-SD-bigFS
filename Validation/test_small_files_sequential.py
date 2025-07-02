# Cenário 1
import time
from utils import create_test_files, drop_caches
import os
import sys
import Pyro5.api
from monitor_resources import KafkaResourceMonitor

# Adiciona o diretório pai ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../client')))
from client import FileSystemClient

def test_sequential_transfer():
    # Configuração
    local_dir = "test_files_small"
    num_files = 1000
    file_size_kb = 512
    
    # Criar arquivos de teste (pode demorar muito)
    print(f"Criando {num_files} arquivos grandes...")
    test_files = create_test_files(local_dir, num_files, file_size_kb)
    drop_caches()
    
    # Inicializar cliente
    nameserver = Pyro5.api.locate_ns()
    client = FileSystemClient(nameserver, "Client-SequentialTest")
    
    monitor = KafkaResourceMonitor()
    monitor.start()
    for filepath in test_files:
        base_name = os.path.basename(filepath)
        remote_path = f"dfs:/{base_name}"
        client.upload_file(filepath, remote_path)


    monitor.stop()
    monitor.show_results()

if __name__ == "__main__":
    test_sequential_transfer()