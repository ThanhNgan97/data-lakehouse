import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_compose_has_ozone_and_no_minio_runtime():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8").lower()
    assert "  s3g:" in compose
    assert "  scm:" in compose
    assert "  om:" in compose
    assert "minio/minio" not in compose
    assert "  minio:" not in compose
    assert "9000:" not in compose
    assert "9001:" not in compose


def test_airflow_image_does_not_install_minio_sdk():
    dockerfile = (ROOT / "airflow.Dockerfile").read_text(encoding="utf-8").lower()
    assert "    minio \\" not in dockerfile
    assert "    boto3 \\" in dockerfile


def test_ozone_validation_dag_has_exact_read_only_validation_chain():
    dag = (ROOT / "lakehouse" / "dags" / "ozone_read_validation.py").read_text(
        encoding="utf-8"
    )
    for task_id in (
        "validate_request",
        "head_ozone_object",
        "download_and_verify_checksum",
        "read_file_sample",
        "publish_validation_result",
    ):
        assert f'task_id="{task_id}"' in dag
    assert "list_objects_v2" not in dag
    assert "delete_object" not in dag
    assert "copy_object" not in dag


def test_python_runtime_uses_storage_neutral_s3_configuration():
    runtime_files = [
        ROOT / "backend" / "api" / "routes" / "upload.py",
        ROOT / "lakehouse" / "dags" / "universal_lakehouse_pipeline.py",
        ROOT / "lakehouse" / "spark" / "env_config.py",
        ROOT / "lakehouse" / "spark" / "bronze_writer.py",
        ROOT / "lakehouse" / "spark" / "checkpoint_store.py",
    ]
    for path in runtime_files:
        content = path.read_text(encoding="utf-8").lower()
        assert "minio" not in content, path
        assert "minioadmin" not in content, path
        assert ":9000" not in content, path


def test_one_command_team_setup_contract_is_present():
    setup = (ROOT / "scripts" / "setup_team.ps1").read_text(encoding="utf-8")
    assert 'Import-RootDotEnv -Override' in setup
    assert 'bootstrap_team.ps1' in setup
    assert 'seed_demo.ps1' in setup
    assert 'verify_team_environment.ps1' in setup
    assert 'SETUP_TEAM=PASS' in setup
    assert 'docker system prune' not in setup
    assert 'docker volume prune' not in setup


def test_team_environment_defaults_match_host_ports_and_current_models():
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "PG_PORT=5433" in env_example
    assert "S3_ENDPOINT=http://127.0.0.1:9878" in env_example
    assert "AIRFLOW_WEBSERVER_URL=http://localhost:8080" in env_example
    assert "GEMINI_DOCUMENT_MODELS=gemini-3.5-flash-lite" in env_example

    compose = "\n".join([
        (ROOT / "docker-compose.yml").read_text(encoding="utf-8"),
        (ROOT / "docker-compose.mock-api.yml").read_text(encoding="utf-8"),
    ])
    compose_variables = set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)", compose))
    env_variables = set(re.findall(r"(?m)^([A-Za-z_][A-Za-z0-9_]*)=", env_example))
    assert compose_variables <= env_variables


def test_seed_accepts_incremental_no_change_replays():
    seed = (ROOT / "scripts" / "seed_demo.ps1").read_text(encoding="utf-8")
    assert 'SEED_DATASET_NO_CHANGE=' in seed
    assert 'TASK_NO_CHANGE' in seed
