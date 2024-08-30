import os
import time
from file_system_manager import FileSystemManager
from az_copy import AzCopy
from logger_manager import LoggerManager

class DirectoryWatch:
    def __init__(self, directory_to_watch, file_system_manager, az_copy, destination_url, processed_files_log, logger=None):
        self.directory_to_watch = directory_to_watch
        self.file_system_manager = file_system_manager
        self.az_copy = az_copy
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
            file_path = os.path.join(self.directory_to_watch, file_name)
            if file_name not in self.files_snapshot or self.files_snapshot[file_name] != mtime:
                if file_path not in self.processed_files:
                    self.logger.log_info(f"Arquivo processado: {file_path}")
                    self.file_system_manager.process_file(file_path, self.az_copy, self.destination_url)
                    self.write_processed_file(file_path)
                    self.processed_files.add(file_path)

        self.files_snapshot = current_snapshot

def main():
    logger = LoggerManager('main.log')
    azcopy_path = r"C:\Program Files\azcopy\azcopy.exe"
    directory_to_watch = r"C:\Users\Admin\Desktop\MauMau\Estudos\Az\AzCopy\ProjetoAzCopyPython\fonte"
    destination_url = "https://azcopypocbrad.blob.core.windows.net/inbound?sp=racwdl&st=2024-08-29T11:52:03Z&se=2024-08-30T19:52:03Z&sv=2022-11-02&sr=c&sig=8IBu0A0ahi1y8TZWOfCqPd4W27KlY%2FXdaQfBlwxmuz8%3D"
    processed_files_log = 'processed_files.txt'

    file_system_manager = FileSystemManager(directory_to_watch, logger)
    az_copy = AzCopy(azcopy_path, file_system_manager, logger, retries=5, retry_delay=30)

    directory_watchdog = DirectoryWatch(directory_to_watch, file_system_manager, az_copy, destination_url, processed_files_log, logger)

    try:
        logger.log_info(f"Observando o diretório: {directory_to_watch}")
        while True:
            directory_watchdog.check_for_changes()
            time.sleep(1)  # Intervalo de tempo entre as verificações
    except KeyboardInterrupt:
        logger.log_info("Interrompido pelo usuário.")

if __name__ == "__main__":
    main()
