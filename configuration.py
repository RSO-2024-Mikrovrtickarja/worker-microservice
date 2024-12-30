from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Configures the ZeroMQ messaging system that
    # is connected to the storage.
    zmq_host: str
    zmq_port: int

    photo_storage_host: str
    photo_storage_port: int

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
