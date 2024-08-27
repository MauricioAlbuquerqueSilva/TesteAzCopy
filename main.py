import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from file_system_manager import FileSystemManager
from az_copy import AzCopy
from logger_manager import LoggerManager

class DirectoryWatchdog(FileSystemEventHandler):
    def __init__(self, file_system_manager, az_copy, destination_url, logger=None):
        self.file_system_manager = file_system_manager
        self.az_copy = az_copy
        self.destination_url = destination_url
        self.logger = logger or LoggerManager('directory_watchdog.log')

    def on_any_event(self, event):
        print(event.is_directory)
        print(event)
    def on_modified(self, event):
        if not event.is_directory:
            self.logger.log_info(f"Arquivo modificado: {event.src_path}")
            self.file_system_manager.process_file(event.src_path, self.az_copy, self.destination_url)

    def on_created(self, event):
        if not event.is_directory:
            self.logger.log_info(f"Novo arquivo criado: {event.src_path}")
            self.file_system_manager.process_file(event.src_path, self.az_copy, self.destination_url)

def main():
    logger = LoggerManager('main.log')
    # windowns
    # azcopy_path = r"C:\Program Files\azcopy\azcopy.exe"
    azcopy_path = "/bin/azcopy"
    # windowns
    # directory_to_watch = r"C:\Users\Admin\Desktop\MauMau\Estudos\Az\AzCopy\ProjetoAzCopyPython\fonte"
    directory_to_watch = "/home/mauricio"
    destination_url = "token  SAS/container inbound "

    file_system_manager = FileSystemManager(directory_to_watch, logger)
    az_copy = AzCopy(azcopy_path, file_system_manager, logger, retries=5, retry_delay=30)

    event_handler = DirectoryWatchdog(file_system_manager, az_copy, destination_url, logger)
    observer = Observer()
    observer.schedule(event_handler, path=directory_to_watch, recursive=False)

    try:
        observer.start()
        logger.log_info(f"Observando o diretório: {directory_to_watch}")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.log_info("Interrompido pelo usuário.")

    observer.join()

if __name__ == "__main__":
    main()
