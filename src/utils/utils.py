from pathlib import Path
import os

def get_base_dir():
    docker_path = Path("/opt/airflow")

    # If running inside Airflow container AND path exists
    if docker_path.exists():
        return docker_path

    # Local development fallback (project root)
    return Path(__file__).resolve().parents[2]