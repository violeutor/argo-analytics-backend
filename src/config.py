from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_key: str
    anthropic_api_key: str = ""       # ANTHROPIC_API_KEY auf Railway setzen
    openregister_api_key: str | None = None  # OPENREGISTER_API_KEY auf Railway setzen
    ba_bridge_url: str = ""           # BA-05: Bridge URL — https://bridge.railway.app
    ba_bridge_api_key: str = ""       # BA-05: BRIDGE_API_KEY aus Bridge .env
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    env: str = "development"

    class Config:
        env_file = ".env"


settings = Settings()
