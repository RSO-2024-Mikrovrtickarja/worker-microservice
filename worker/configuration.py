from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Configures the ZeroMQ messaging system that
    # is connected to the storage.
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_queue_name: str

    photo_storage_host: str
    photo_storage_port: int

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
