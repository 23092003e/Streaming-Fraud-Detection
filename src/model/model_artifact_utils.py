import argparse
import json
import shutil
import tempfile
from pathlib import Path


REQUIRED_STAGE_DIRS = {
    "org.apache.spark.ml.feature.StringIndexerModel": ["data"],
    "org.apache.spark.ml.classification.RandomForestClassificationModel": ["data", "treesMetadata"],
}


def remove_crc_files(model_path: Path) -> None:
    for crc_file in model_path.rglob("*.crc"):
        crc_file.unlink()


def _load_metadata_class(metadata_file: Path) -> str:
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    return metadata["class"]


def validate_model_artifact(model_path: Path) -> None:
    metadata_file = model_path / "metadata" / "part-00000"
    if not metadata_file.exists():
        raise ValueError(f"Missing model metadata file: {metadata_file}")

    stages_dir = model_path / "stages"
    if not stages_dir.exists():
        raise ValueError(f"Missing stages directory: {stages_dir}")

    missing_paths = []
    for stage_dir in sorted(p for p in stages_dir.iterdir() if p.is_dir()):
        stage_metadata = stage_dir / "metadata" / "part-00000"
        if not stage_metadata.exists():
            missing_paths.append(str(stage_metadata))
            continue

        stage_class = _load_metadata_class(stage_metadata)
        for required_name in REQUIRED_STAGE_DIRS.get(stage_class, []):
            required_path = stage_dir / required_name
            if not required_path.exists():
                missing_paths.append(str(required_path))

    if missing_paths:
        joined = "\n".join(missing_paths)
        raise ValueError(f"Model artifact is incomplete. Missing required paths:\n{joined}")


def export_and_validate_pipeline_model(model, export_path: Path) -> None:
    export_path = Path(export_path)
    export_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_export_path = Path(tmpdir) / export_path.name
        model.write().overwrite().save(str(temp_export_path))
        remove_crc_files(temp_export_path)
        validate_model_artifact(temp_export_path)

        if export_path.exists():
            shutil.rmtree(export_path)
        shutil.copytree(temp_export_path, export_path)

    remove_crc_files(export_path)
    validate_model_artifact(export_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a Spark ML model artifact.")
    parser.add_argument("model_path", help="Path to the model directory")
    args = parser.parse_args()

    model_path = Path(args.model_path)
    remove_crc_files(model_path)
    validate_model_artifact(model_path)
    print(f"Model artifact is valid: {model_path}")


if __name__ == "__main__":
    main()
