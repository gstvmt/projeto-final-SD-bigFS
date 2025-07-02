# Cenário 8
import threading
import time
from queue import Queue
from utils import create_test_files, calculate_checksum
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from client import Client

def mixed_worker(task_queue, result_queue):
    client = Client()
    while True:
        task = task_queue.get()
        if task is None:
            break
            
        filepath, operation = task
        filename = os.path.basename(filepath)
        
        try:
            if operation == "upload":
                remote_path = "remoto:/" + filename
                start_time = time.time()
                result = client.copy([filepath, remote_path])
                transfer_time = time.time() - start_time
                result_queue.put((filename, "upload", transfer_time, result))
                
            elif operation == "download":
                remote_path = "remoto:/" + filename
                local_copy = filepath + ".copy"
                start_time = time.time()
                result = client.copy([remote_path, local_copy])
                transfer_time = time.time() - start_time
                if os.path.exists(local_copy):
                    os.remove(local_copy)
                result_queue.put((filename, "download", transfer_time, result))
                
        except Exception as e:
            result_queue.put((filename, operation, 0, f"Erro: {str(e)}"))
            
        task_queue.task_done()

def test_mixed_transfer():
    # Configuração
    small_dir = "test_files_mixed_small"
    large_dir = "test_files_mixed_large"
    num_small_files = 100
    num_large_files = 200
    small_size_kb = 512  # 0.5MB
    large_size_mb = 1024  # 1GB
    num_threads = 20
    
    # Criar arquivos de teste
    print("Criando arquivos pequenos...")
    small_files = create_test_files(small_dir, num_small_files, small_size_kb)
    
    print("Criando arquivos grandes (isso pode demorar)...")
    large_files = create_test_files(large_dir, num_large_files, large_size_mb * 1024)
    
    # Preparar filas
    task_queue = Queue()
    result_queue = Queue()
    
    # Criar workers
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=mixed_worker, args=(task_queue, result_queue))
        t.start()
        threads.append(t)
    
    # Adicionar tarefas de upload (mistura de pequenos e grandes)
    for i, filepath in enumerate(small_files[:50] + large_files[:100]):
        task_queue.put((filepath, "upload"))
    
    # Adicionar tarefas de download (mistura de pequenos e grandes)
    for i, filepath in enumerate(small_files[50:100] + large_files[100:200]):
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
    
    small_uploads = [r for r in uploads if "small" in r[0]]
    large_uploads = [r for r in uploads if "large" in r[0]]
    small_downloads = [r for r in downloads if "small" in r[0]]
    large_downloads = [r for r in downloads if "large" in r[0]]
    
    print("\n=== Resultados ===")
    print(f"Uploads pequenos completos: {len(small_uploads)}/50")
    print(f"Uploads grandes completos: {len(large_uploads)}/100")
    print(f"Downloads pequenos completos: {len(small_downloads)}/50")
    print(f"Downloads grandes completos: {len(large_downloads)}/100")
    
    if small_uploads:
        avg_time = sum(r[2] for r in small_uploads) / len(small_uploads)
        print(f"\nTempo médio upload pequenos: {avg_time:.4f} segundos")
    
    if large_uploads:
        avg_time = sum(r[2] for r in large_uploads) / len(large_uploads)
        print(f"Tempo médio upload grandes: {avg_time:.4f} segundos")
    
    if small_downloads:
        avg_time = sum(r[2] for r in small_downloads) / len(small_downloads)
        print(f"Tempo médio download pequenos: {avg_time:.4f} segundos")
    
    if large_downloads:
        avg_time = sum(r[2] for r in large_downloads) / len(large_downloads)
        print(f"Tempo médio download grandes: {avg_time:.4f} segundos")
    
    # Verificar falhas
    failures = [r for r in results if "sucesso" not in r[3]]
    if failures:
        print(f"\nFalhas encontradas: {len(failures)}")
        for f in failures[:10]:  # Mostrar apenas as primeiras 10 falhas
            print(f"- {f[0]} ({f[1]}): {f[3]}")

if __name__ == "__main__":
    test_mixed_transfer()