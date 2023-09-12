import os
from pathlib import Path

from dagster import Config, RunConfig, RunRequest, job, op, sensor, define_asset_job, asset

MY_DIRECTORY = Path(__file__).parent.parent / "data"

class FileConfig(Config):
    filename: str


@op
def process_file(context, config: FileConfig):
    context.log.info(config.filename)


@job
def log_file_job():
    process_file()

asset_job = define_asset_job("asset_job", "*")


@sensor(job=asset_job)
def my_directory_sensor():
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config=RunConfig(
                    ops={"process_file": FileConfig(filename=filename)}
                ),
            )


@asset
def ret1():
    return 1