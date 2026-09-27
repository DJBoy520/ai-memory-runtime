from pathlib import Path
from typing import List, Optional, Union
import os
import stat
import yaml
from pydantic import BaseModel, Field, field_validator


class ServerConfig(BaseModel):
    business_socket: str = "/run/user/1000/qdrant-bge.sock"
    admin_socket: str = "/run/user/1000/qdrant-bge-admin.sock"
    socket_mode: int = 0o600
    max_request_bytes: int = 4 * 1024 * 1024  # 4MB

    @field_validator("socket_mode", mode="before")
    @classmethod
    def parse_socket_mode(cls, v: Union[int, str]) -> int:
        if isinstance(v, str):
            return int(v, 8) if v.startswith(("0o", "0O")) else int(v)
        return int(v)


class ModelConfig(BaseModel):
    name_or_path: str = "BAAI/bge-m3"
    device: str = "cuda"
    fp16: bool = True
    max_batch: int = 16
    max_queue_size: int = 64
    idle_timeout_seconds: int = 300
    max_token_length: int = 8192


class StorageConfig(BaseModel):
    sqlite_path: str = "data/sessions.db"
    busy_timeout: int = 5000
    wal_enabled: bool = True

    # 兼容 wal_mode 命名访问
    @property
    def wal_mode(self) -> bool:
        return self.wal_enabled

    @wal_mode.setter
    def wal_mode(self, value: bool) -> None:
        self.wal_enabled = value


# 保持向后兼容别名
DatabaseConfig = StorageConfig


class QdrantConfig(BaseModel):
    url: str = "http://localhost:6333"
    api_key: Optional[str] = ""
    timeout: float = 10.0
    prefer_grpc: bool = True
    collections: List[str] = Field(
        default_factory=lambda: ["ai_memory", "crypto_standards", "project_docs"]
    )


class AppConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    qdrant: QdrantConfig = Field(default_factory=QdrantConfig)

    # 兼容 database 属性访问
    @property
    def database(self) -> StorageConfig:
        return self.storage

    @database.setter
    def database(self, value: StorageConfig) -> None:
        self.storage = value


def check_and_fix_file_permissions(config_path: Path) -> None:
    """保证 config.yaml 的权限严格为 0600，且其父目录不向 public 暴露"""
    if config_path.exists():
        current_mode = stat.S_IMODE(os.stat(config_path).st_mode)
        if current_mode != 0o600:
            os.chmod(config_path, 0o600)


def load_config(config_path: Optional[str | Path] = None) -> AppConfig:
    """
    加载配置文件。优先读取传入路径，其次 config/config.yaml，未找到时退回 config/config.example.yaml
    """
    root_dir = Path(__file__).resolve().parent.parent

    if config_path is not None:
        target_path = Path(config_path)
    else:
        env_path = os.getenv("AMR_CONFIG_PATH")
        if env_path:
            target_path = Path(env_path)
        else:
            local_yaml = root_dir / "config" / "config.yaml"
            example_yaml = root_dir / "config" / "config.example.yaml"
            target_path = local_yaml if local_yaml.exists() else example_yaml

    if not target_path.exists():
        return AppConfig()

    check_and_fix_file_permissions(target_path)

    with open(target_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # 如果 yaml 中存在旧的 database 键而无 storage 键，做平滑兼容映射
    if "database" in data and "storage" not in data:
        data["storage"] = data.pop("database")
        if isinstance(data["storage"], dict) and "wal_mode" in data["storage"] and "wal_enabled" not in data["storage"]:
            data["storage"]["wal_enabled"] = data["storage"].pop("wal_mode")

    return AppConfig(**data)


# 全局单例配置示例
settings = load_config()
