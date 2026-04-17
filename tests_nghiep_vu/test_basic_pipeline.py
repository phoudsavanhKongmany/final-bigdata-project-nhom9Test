from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_required_source_files_exist():
    required = [
        ROOT / "src" / "ingestion.py",
        ROOT / "src" / "storage.py",
        ROOT / "src" / "processing.py",
        ROOT / "src" / "main_pipeline.py",
    ]
    missing = [str(p) for p in required if not p.exists()]
    assert not missing, f"Missing required pipeline files: {missing}"


def test_main_pipeline_help_runs_successfully():
    cmd = [sys.executable, str(ROOT / "src" / "main_pipeline.py"), "--help"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    assert result.returncode == 0
    assert "--project" in result.stdout
    assert "--step" in result.stdout


def test_requirements_include_pyspark():
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "pyspark" in requirements.lower()
