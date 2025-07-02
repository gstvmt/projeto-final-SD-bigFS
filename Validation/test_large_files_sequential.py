# Cenário 5
import time
from utils import create_test_files, drop_caches
import os
import sys
import Pyro5.api
from monitor_resources import KafkaResourceMonitor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../client')))
from client import FileSystemClient

def test_sequential_large_transfer():
    # Configuração
    local_dir = "test_files_large"
    num_files = 10
    file_size_mb = 2048  # 2GB
    
    # Criar arquivos de teste (pode demorar muito)
    print(f"Criando {num_files} arquivos grandes...")
    test_files = create_test_files(local_dir, num_files, file_size_mb * 1024)
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
    test_sequential_large_transfer()