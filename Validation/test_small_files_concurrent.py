# Cenário 2
import threading
import time
from queue import Queue
from utils import create_test_files, calculate_checksum, drop_caches
import os
import sys
import Pyro5.api
from monitor_resources import KafkaResourceMonitor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../client')))
from client import FileSystemClient

task_queue = Queue()

class VirtualUser(threading.Thread):
    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id

    def run(user_id):
        nameserver = Pyro5.api.locate_ns()
        client = FileSystemClient(nameserver, "Client-ConcurrentTest")

        while True:
            task = task_queue.get()
            if task is None:
                break
                
            filepath, direction = task
            filename = os.path.basename(filepath)
            
            if direction == "upload":
                remote_path = f"dfs:/{filename}"
                client.upload_file(filepath, remote_path)

            elif direction == "download":
                remote_path = f"dfs:/{filename}"
                local_copy_path = f"test_files_concurrent/copy/{filename}"
                
                os.makedirs(os.path.dirname(local_copy_path), exist_ok=True)
                client.download_file(remote_path, local_copy_path)

            task_queue.task_done()

def test_concurrent_transfer():
    # Configuração
    local_dir = "test_files_concurrent"
    num_files_upload = 1000
    num_files_download = 500
    file_size_kb = 256
    num_threads = 1
    
    # Criar arquivos de teste
    print("Criando arquivos de teste...")
    test_files = create_test_files(local_dir, num_files_upload, file_size_kb)
    
    drop_caches()
    
    monitor = KafkaResourceMonitor()
    monitor.start()

    # Criar threads
    threads = []
    for user_id in range(num_threads):
        user = VirtualUser(user_id)
        user.start()
        threads.append(user)
    
    # Adicionar tarefas de upload
    for filepath in test_files[:num_files_upload]:
        task_queue.put((filepath, "upload"))
    
    # Adicionar tarefas de download (usando os primeiros arquivos)
    for filepath in test_files[:num_files_download]:
        task_queue.put((filepath, "download"))
    
    # Esperar conclusão
    task_queue.join()
    
    # Encerrar workers
    for i in range(num_threads):
        task_queue.put(None)
    for t in threads:
        t.join()
    
    monitor.stop()
    monitor.show_results()

    verify = True
    if verify:
        print("Calculando checksum local...")
        original_checksums = {os.path.basename(f): calculate_checksum(f) for f in test_files[:num_files_download]}
    
        local_dir = "test_files_concurrent/copy/"
        final_checksums = {}
        for filename in os.listdir(local_dir):
            filepath = os.path.join(local_dir, filename)
            if os.path.isfile(filepath):
                final_checksums[filename] = calculate_checksum(filepath)

        count = 0
        for filename, original_checksum in original_checksums.items():
            final_checksum = final_checksums.get(filename)
            if final_checksum != original_checksum:
                count += 1
                print(f"Checksum falhou para {filename} - Arquivo corrompido")

        print(f"Verificação de integridade concluída. {count} arquivos corrompidos.")

if __name__ == "__main__":
    test_concurrent_transfer()