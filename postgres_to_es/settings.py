from pydantic import BaseSettings, Field
import logging


class PgSettings(BaseSettings):
    dbname: str = Field(env='POSTGRES_DB')
    user: str = Field(env='POSTGRES_USER')
    password: str = Field(env='POSTGRES_PASSWORD')
    host: str = Field(env='DB_HOST')
    port: int = Field(env='DB_PORT')

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class EsSettings(BaseSettings):
    scheme: str = Field(env='ES_SCHEME')
    host: str = Field(env='ES_HOST')
    port: int = Field(env='ES_PORT')
    index_name: str = Field(env='INDEX_NAME')

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class ETLSettings(BaseSettings):
    batch_size: int = Field(100, env='BATCH_SIZE')
    timeout: int = Field(60, env='TIMEOUT')
    state_file: str = Field('storage.txt', env='STATE_FILE')
    backoff_max_time: int = Field(env='BACKOFF_MAX_TIME')
    log_level: str = Field(env='LOG_LEVEL')

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


etl_settings = ETLSettings()
pg_settings = PgSettings()
es_settings = EsSettings()


logger = logging.getLogger(__name__)
logger.setLevel(etl_settings.log_level)

handler = logging.StreamHandler()
handler.setLevel(etl_settings.log_level)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)
