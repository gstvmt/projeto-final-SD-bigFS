import os
import hashlib
import random
import string
import subprocess

def generate_random_file(path, size_kb):
    """Gera um arquivo binário com o caractere 'A' repetido, com tamanho especificado em KB."""
    with open(path, 'wb') as f:
        chunk = b'A' * 1024  # 1 KB de 'A's
        for _ in range(size_kb):
            f.write(chunk)
        
def calculate_checksum(filepath):
    """Calcula o checksum SHA256 de um arquivo"""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def create_test_files(directory, num_files, size_kb):
    """Cria vários arquivos de teste"""
    os.makedirs(directory, exist_ok=True)
    for i in range(num_files):
        filepath = os.path.join(directory, f"testfile_{i}.txt")
        generate_random_file(filepath, size_kb)
    return [os.path.join(directory, f) for f in os.listdir(directory)]

def drop_caches():
    # Sincroniza o sistema de arquivos (flush)
    subprocess.run(["sync"])
    # Limpa page cache, dentries e inodes
    subprocess.run(["sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches"])

