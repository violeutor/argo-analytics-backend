from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_key: str
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    env: str = "development"

    class Config:
        env_file = ".env"


settings = Settings()
