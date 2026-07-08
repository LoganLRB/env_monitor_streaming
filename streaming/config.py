import os

from pydantic_settings import BaseSettings, SettingsConfigDict

_SSM_PREFIX = "/env-monitor-streaming"


def _load_ssm_env() -> None:
    if os.environ.get("ENVIRONMENT", "local").lower() == "local":
        return
    try:
        import boto3
        client = boto3.client("ssm")
        resp = client.get_parameters_by_path(Path=_SSM_PREFIX + "/", Recursive=False)
        for param in resp.get("Parameters", []):
            key = param["Name"].rsplit("/", 1)[-1]
            os.environ.setdefault(key, param["Value"])
    except Exception as exc:
        print(f"[config] WARNING: could not load SSM parameters from {_SSM_PREFIX}/: {exc}")


_load_ssm_env()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "local"
    api_base_url: str = "http://localhost:8000"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "sensor.readings"

    redis_url: str = "redis://localhost:6379"

    sns_alert_topic_arn: str = ""

    @property
    def is_local(self) -> bool:
        return self.environment.lower() == "local"


settings = Settings()
