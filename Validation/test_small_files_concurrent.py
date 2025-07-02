# Cenário 2
import threading
import time
from queue import Queue
from utils import create_test_files, calculate_checksum, drop_caches
import os
import sys
import psutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from client import Client

def worker(task_queue, result_queue):
    client = Client()
    while True:
        task = task_queue.get()
        if task is None:
            break
            
        filepath, direction = task
        filename = os.path.basename(filepath)
        
        if direction == "upload":
            remote_path = "remoto:."
            local_checksum = calculate_checksum(filepath)
            start_time = time.time()
            result = client.copy([filepath, remote_path])
            transfer_time = time.time() - start_time
            result_queue.put((filename, "upload", transfer_time, result))
            
        elif direction == "download":
            remote_path = "remoto:" + filename
            local_copy_path = "test_files_concurrent/copy"
            start_time = time.time()
            result = client.copy([remote_path, local_copy_path])
            transfer_time = time.time() - start_time
            local_copy_path = os.path.join(local_copy_path, filename)
            if os.path.exists(local_copy_path):
                os.remove(local_copy_path)
            result_queue.put((filename, "download", transfer_time, result))
            
        task_queue.task_done()

def test_concurrent_transfer():
    # Configuração
    local_dir = "test_files_concurrent"
    num_files_upload = 1000
    num_files_download = 500
    file_size_kb = 256
    num_threads = 10
    
    # Criar arquivos de teste
    print("Criando arquivos de teste...")
    test_files = create_test_files(local_dir, num_files_upload, file_size_kb)
    
    # Preparar filas
    task_queue = Queue()
    result_queue = Queue()
    
    drop_caches()
    process = psutil.Process(os.getpid())
    cpu_start = process.cpu_times()
    io_start = process.io_counters()

    
    # Criar workers
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(task_queue, result_queue))
        t.start()
        threads.append(t)
    
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
    
    # Processar resultados
    results = list(result_queue.queue)
    
    # Relatório
    uploads = [r for r in results if r[1] == "upload"]
    downloads = [r for r in results if r[1] == "download"]
    
    cpu_end = process.cpu_times()
    io_end = process.io_counters()

    print("\n=== Resultados ===")
    print(f"Uploads completos: {len(uploads)}/{num_files_upload}")
    print(f"Downloads completos: {len(downloads)}/{num_files_download}")

    print("\n=== Recursos do processo ===")
    print(f"Tempo total de CPU (user): {cpu_end.user - cpu_start.user:.2f} s")
    print(f"Tempo total de CPU (system): {cpu_end.system - cpu_start.system:.2f} s")
    print(f"Leituras de disco: {io_end.read_count - io_start.read_count}")
    print(f"Escritas de disco: {io_end.write_count - io_start.write_count}")
    print(f"Bytes lidos: {(io_end.read_bytes - io_start.read_bytes) / 1024:.2f} KB")
    print(f"Bytes escritos: {(io_end.write_bytes - io_start.write_bytes) / 1024:.2f} KB")
    
    if uploads:
        avg_upload = sum(r[2] for r in uploads) / len(uploads)
        print(f"Tempo médio de upload: {avg_upload:.4f} segundos")
    
    if downloads:
        avg_download = sum(r[2] for r in downloads) / len(downloads)
        print(f"Tempo médio de download: {avg_download:.4f} segundos")
    
    # Verificar falhas
    failures = [r for r in results if "sucesso" not in r[3]]
    if failures:
        print(f"\nFalhas encontradas: {len(failures)}")
        for f in failures[:10]:  # Mostrar apenas as primeiras 10 falhas
            print(f"- {f[0]} ({f[1]}): {f[3]}")

if __name__ == "__main__":
    test_concurrent_transfer()