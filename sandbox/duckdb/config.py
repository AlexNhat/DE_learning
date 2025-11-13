from dataclasses import dataclass, asdict
import duckdb


@dataclass
class DuckDBConfig:
    """General DuckDB configuration class for the project."""

    env: str = "dev"
    python_enable_replacements: bool = False
    enable_external_access: bool = True
    preserve_insertion_order: bool = False
    threads: int = 4
    data_size: int = 1_000_000
    memory_limit: str = "2GB"
    enable_progress_bar: bool = True

    def validate(self):
        """Validate configuration before applying."""
        valid_envs = {"dev", "prod", "test"}
        if self.env not in valid_envs:
            raise ValueError(f"Invalid env '{self.env}'. Must be one of {valid_envs}")

        if not isinstance(self.threads, int) or self.threads < 0:
            raise ValueError("threads must be a non-negative integer")

        if not isinstance(self.memory_limit, str) or not any(
            unit in self.memory_limit.upper() for unit in ("MB", "GB", "%")
        ):
            raise ValueError(
                "memory_limit must include a valid unit (e.g., '1GB', '512MB', '50%')"
            )
        return True

    def to_dict(self):
        """Return the configuration as a dictionary."""
        return asdict(self)

    def __repr__(self):
        """Readable string representation of the configuration."""
        return f"<DuckDBConfig env={self.env} threads={self.threads} memory={self.memory_limit}>"
