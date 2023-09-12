import os
from pathlib import Path

from dagster import AssetSelection, Config, RunConfig, RunRequest, job, op, sensor, define_asset_job, asset, DefaultSensorStatus

FILEWATCHING_DIR = Path(__file__).parent.parent / "watchdir"
INPUT_DIR = FILEWATCHING_DIR / "input"
OUTPUT_DIR = FILEWATCHING_DIR / "output"

assert INPUT_DIR.is_dir(), f"input dir not found {INPUT_DIR}"
assert OUTPUT_DIR.is_dir(), f"output dir not found {OUTPUT_DIR}"

class FileConfig(Config):
    filename: str


@op
def process_file(context, config: FileConfig):
    context.log.info(config.filename)


@job
def log_file_job():
    process_file()

@asset
def ret1():
    return 1


@asset
def ret2(ret1):
    return 2

asset_job = define_asset_job(name="asset_job", selection=AssetSelection.keys("ret1", "ret2"))


@sensor(job=asset_job, default_status=DefaultSensorStatus.RUNNING)
def my_directory_sensor():
    for filename in os.listdir(INPUT_DIR):
        filepath = os.path.join(INPUT_DIR, filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                # run_config=RunConfig(
                    # ops={"process_file": FileConfig(filename=filename)}
                # ),
            )

