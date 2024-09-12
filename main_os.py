import os
import time
import subprocess
from file_system_manager import FileSystemManager
from az_copy import AzCopy
from logger_manager import LoggerManager

class DirectoryWatch:
    def __init__(self, directory_to_watch, file_system_manager, az_copy, destination_url, token, processed_files_log, logger=None):
        self.directory_to_watch = directory_to_watch
        self.file_system_manager = file_system_manager
        self.az_copy = az_copy
        self.token = token
        self.destination_url = destination_url
        self.processed_files_log = processed_files_log
        self.ensure_log_file_exists()
        self.logger = logger or LoggerManager('directory_watch.log')
        self.files_snapshot = self.take_snapshot()
        self.processed_files = self.load_processed_files()

    def ensure_log_file_exists(self):
        """Garante que o arquivo de log de arquivos processados exista."""
        if not os.path.exists(self.processed_files_log):
            with open(self.processed_files_log, 'w') as file:
                pass 

    def take_snapshot(self):
        """Cria uma lista dos arquivos atuais no diretório."""
        return {f: os.path.getmtime(os.path.join(self.directory_to_watch, f)) for f in os.listdir(self.directory_to_watch) if os.path.isfile(os.path.join(self.directory_to_watch, f))}

    def load_processed_files(self):
        """Carrega a lista de arquivos já processados a partir do arquivo de log."""
        if os.path.exists(self.processed_files_log):
            with open(self.processed_files_log, 'r') as file:
                return set(file.read().splitlines())
        return set()

    def write_processed_file(self, file_path):
        """Escreve o nome do arquivo processado no arquivo de log."""
        with open(self.processed_files_log, 'a') as file:
            file.write(file_path + '\n')

    def check_for_changes(self):
        """Verifica as mudanças no diretório e processa os arquivos conforme necessário."""
        current_snapshot = self.take_snapshot()
        for file_name, mtime in current_snapshot.items():
            available_memory = self.file_system_manager.get_available_memory()
            print(f'Sistema operacional: {os.name} memoria disponivel: {available_memory}')
            if (os.name == 'posix' and available_memory is not None and available_memory > 500) or (os.name == 'nt'):
                file_path = os.path.join(self.directory_to_watch, file_name)
                if file_name not in self.files_snapshot or self.files_snapshot[file_name] != mtime:
                    if file_path not in self.processed_files:
                        self.logger.log_info(f"Arquivo processado: {file_path}")
                        self.file_system_manager.process_file(file_path, self.az_copy, self.destination_url, self.token)
                        self.file_system_manager.process_error_files(self.az_copy)
                        self.write_processed_file(file_path)
                        self.processed_files.add(file_path)
            else: 
                self.logger.log_info(f"Memória da maquina insuficiente para processar os arquivos: {available_memory}")

        self.files_snapshot = current_snapshot

def main():
    logger = LoggerManager('main.log')
    
    azcopy_path = "/bin/azcopy"
    directory_to_watch = "./ingestion"
    
    # openssl req -x509 -newkey rsa:2048 -keyout private_key.pem -out cert.pem -days 365

    destination_url = "https://azcopypocbrad.blob.core.windows.net"
    token = "?sp=racwdl&st=2024-09-11T21:01:30Z&se=2024-09-20T05:01:30Z&sv=2022-11-02&sr=c&sig=gzrvPjmxxGMo3cwtTZeMFWSK1pJr4ASPeOi5oQOEy3k%3D"

    processed_files_log = 'processed_files.txt'

    subprocess.run('export AZCOPY_LOG_LOCATION="mnt/c/Users/Admin/Desktop/MauMau/Estudos/Az/AzCopy/ProjetoAzCopyPython"', shell=True)

    file_system_manager = FileSystemManager(directory_to_watch, logger)
    az_copy = AzCopy(azcopy_path, file_system_manager, logger, retries=1, retry_delay=10)

    directory_watch = DirectoryWatch(directory_to_watch, file_system_manager, az_copy, destination_url, token, processed_files_log, logger)

    try:
        logger.log_info(f"Observando o diretório: {directory_to_watch}")
        while True:
            directory_watch.check_for_changes()
            time.sleep(5)  # Intervalo de tempo entre as verificações
    except KeyboardInterrupt:
        logger.log_info("Interrompido pelo usuário.")

if __name__ == "__main__":
    main()
