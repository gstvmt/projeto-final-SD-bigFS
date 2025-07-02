# Cenário 5
import time
from utils import create_test_files, drop_caches
import os
import sys
from monitor_resources import ResourceMonitor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 
from client import Client

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
    client = Client()
    
    # Testar transferência
    results = []
    total_time = 0
    
    monitor = ResourceMonitor()
    monitor.start()
    for filepath in test_files:
        filename = os.path.basename(filepath)
        remote_path = "remoto:"
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        
        print(f"\nTransferindo {filename} ({file_size:.2f} MB)...")
        start_time = time.time()
        result = client.copy([filepath, remote_path])
        transfer_time = time.time() - start_time
        
        if "sucesso" not in result:
            print(f"Falha ao transferir {filename}: {result}")
            results.append((filename, False, transfer_time, "Falha na transferência"))
            continue
        
        throughput = file_size / transfer_time  # MB/s
        results.append((filename, True, transfer_time, throughput))
        total_time += transfer_time
        
        print(f"Concluído em {transfer_time:.2f} segundos ({throughput:.2f} MB/s)")
    
    monitor.stop()
    # Relatório
    print("\n=== Resultados ===")
    success_count = sum(1 for r in results if r[1])
    print(f"Arquivos transferidos com sucesso: {success_count}/{num_files}")
    print(f"Tempo total: {total_time:.2f} segundos")
    
    if success_count > 0:
        avg_time = total_time / success_count
        avg_throughput = sum(r[3] for r in results if r[1]) / success_count
        print(f"Tempo médio por arquivo: {avg_time:.2f} segundos")
        print(f"Throughput médio: {avg_throughput:.2f} MB/s")

    monitor.show_results()
if __name__ == "__main__":
    test_sequential_large_transfer()