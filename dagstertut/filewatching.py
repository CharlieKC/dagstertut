import os
from pathlib import Path

from dagster import AssetSelection, Config, RunConfig, RunRequest, job, op, sensor, define_asset_job, asset, DefaultSensorStatus

FILEWATCHING_DIR = Path(__file__).parent.parent / "watchdir"
INPUT_DIR = FILEWATCHING_DIR / "input"
INPUT2_DIR = FILEWATCHING_DIR / "input_2"
OUTPUT_DIR = FILEWATCHING_DIR / "output"

assert INPUT_DIR.is_dir(), f"input dir not found {INPUT_DIR}"
assert OUTPUT_DIR.is_dir(), f"output dir not found {OUTPUT_DIR}"

class FileConfig(Config):
    filepath: str



@asset
def ret1(context, config: FileConfig):
    context.log.info(f"File name {config.filepath}")
    
    return config.filepath


@asset
def ret2(context, ret1):
    context.log.info(f"Found the filename in the second asset {ret1}")
    file_txt = OUTPUT_DIR / "files.txt"
    with open(file_txt, 'a') as f:
        f.write(str(ret1) + '\n')

    return 2

@op
def fileinfo_op(context, ret1):
    context.log.info(f"Op received file {ret1}")
    return "WOOO: " + ret1




asset_job = define_asset_job(name="filewatching_job", selection=AssetSelection.keys("ret1", "ret2"))


@sensor(job=asset_job, default_status=DefaultSensorStatus.RUNNING)
def my_directory_sensor():
    filepaths = sorted(list(INPUT_DIR.resolve().glob('*')))
    for filepath in filepaths:
        yield RunRequest(
            run_key=str(filepath),
            run_config=RunConfig(
                ops={"ret1": FileConfig(filepath=str(filepath))}
            ),
        )

