import os
import json
import hashlib
from logger_manager import LoggerManager

class FileSystemManager:
    def __init__(self, directory_to_watch, logger=None):
        self.directory_to_watch = directory_to_watch
        self.logger = logger or LoggerManager('file_system_manager.log')

    def get_available_memory(self):
        available_memory = None
        if 'posix' in os.name:
            meminfo = {}
            with open('/proc/meminfo') as f:
                for line in f:
                    parts = line.split()
                    meminfo[parts[0].strip(':')] = int(parts[1])  # Memória em KB
            
            available_memory = meminfo['MemAvailable'] / 1024  # Convertendo para MB
        return available_memory
    
    def generate_file_hash(self, file_path, hash_algo='sha256'):
        """Gera o hash do arquivo local usando o algoritmo especificado."""
        try:
            hash_func = hashlib.new(hash_algo)
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_func.update(chunk)
            file_hash = hash_func.hexdigest()
            self.logger.log_info(f"Hash do arquivo '{file_path}' gerado com sucesso: {file_hash}")
            return file_hash
        except Exception as e:
            self.logger.log_error(f"Erro ao gerar hash do arquivo '{file_path}': {e}")
            raise e

    def file_exists(self, file_path):
        """Verifica se o arquivo existe."""
        exists = os.path.exists(file_path)
        self.logger.log_info(f"Verificação de existência do arquivo '{file_path}': {'Existe' if exists else 'Não existe'}")
        return exists

    def check_read_permissions(self, file_path):
        """Verifica se o arquivo tem permissões de leitura."""
        readable = os.access(file_path, os.R_OK)
        self.logger.log_info(f"Verificação de permissão de leitura do arquivo '{file_path}': {'Permitido' if readable else 'Negado'}")
        return readable

    def check_write_permissions(self, file_path):
        """Verifica se o arquivo tem permissões de escrita."""
        writable = os.access(file_path, os.W_OK)
        self.logger.log_info(f"Verificação de permissão de escrita do arquivo '{file_path}': {'Permitido' if writable else 'Negado'}")
        return writable

    def load_ingestion_file(self, file_path):
        """Carrega o arquivo de ingestão."""
        with open(file_path, 'r') as file:
            data = json.load(file) 
        return data
    
    def write_log_error_file(self, path, data):
        with open(path, 'w') as json_file:
            json.dump(data, json_file, indent=4)

    def validate_file(self, file_path):
        """Realiza todas as validações necessárias para o arquivo."""
        try:
            if not self.file_exists(file_path):
                raise FileNotFoundError(f"O arquivo '{file_path}' não foi encontrado.")
            
            if not self.check_read_permissions(file_path):
                raise PermissionError(f"Permissão de leitura negada para o arquivo '{file_path}'.")
    
            if not self.check_write_permissions(file_path):
                raise PermissionError(f"Permissão de escrita negada para o arquivo '{file_path}'.")
            
            databases_to_ingest = self.load_ingestion_file(file_path)
            self.logger.log_info(f"Arquivo '{file_path}' validado com sucesso.")

            return databases_to_ingest
        
        except (FileNotFoundError, PermissionError) as e:
            self.logger.log_error(f"Erro na validação do arquivo '{file_path}': {e}")
            raise e

    def process_file(self, file_path, az_copy, destination_url, token):
        """Valida e processa o arquivo, copiando-o para o Azure."""
        try:
            data_to_ingest = self.validate_file(file_path)
            for ing in data_to_ingest:
                ingestion_id = ing['id']
                cloud_location_container = ing['cloud_location_container']
                cloud_destination_folder_name = ing['cloud_destination_folder_name']
                database_path = ing['path_db']
                tables = [] 
                if self.file_exists(f"./dbs/{database_path}"):
                    for table in os.listdir(f"./dbs/{database_path}"):
                        tables.append({
                            "table": table,
                            "partitions": os.listdir(f"./dbs/{database_path}/{table}")
                        })
                else:
                    self.logger.log_error(f"Erro na validação do arquivo './dbs/{database_path}': Arquivo não encontrado")

                for table in tables:
                    self.logger.log_info(f"Processando tabela: {table['table']}")
                    for part in table["partitions"]:
                        self.logger.log_info(f"processando Particao: {part}")
                        cloud_destination = f"{destination_url}/{cloud_location_container}/{cloud_destination_folder_name}/{table['table']}{token}"
                        source_destination = f"./dbs/{database_path}/{table['table']}/{part}"
                        successfully_copied = az_copy.copy_to_azure(source_destination, cloud_destination)
                        if not successfully_copied:
                            log_file = {
                                "ingestion_file": file_path,
                                "ingestion_item": ing,
                                "ingestion_partition": f"{table['table']}/{part}",
                                "partition_source": source_destination,
                                "partition_destination": cloud_destination
                            }
                            self.write_log_error_file(f'./ingestion_error/error_{ingestion_id}_{database_path}_{table["table"]}_{part}.json',log_file)
                            continue
        except Exception as e:
            self.logger.log_error(f"Erro ao processar a ingestão '{file_path}': {e}")
            raise e

    def process_error_files(self, az_copy):
        for error_ingestion in os.listdir('./ingestion_error/'):
            retry_to_ingest = self.load_ingestion_file(f"./ingestion_error/{error_ingestion}")    
            successfully_copied = az_copy.copy_to_azure(retry_to_ingest['partition_source'], retry_to_ingest['partition_destination'])
            if not successfully_copied:
                raise Exception('Processo de ingestão falhou')

