import subprocess
import toolsGeneral.main as tgm
from pathlib import Path
import os
import datetime
from botocore.exceptions import ClientError
import re

def upload_dir_files_to_backblaze(dir:Path, config):

    s3 = config['s3']
    logger = config['logger']
    root = config['root']
    dir_files = [f for f in dir.rglob("*") if f.is_file()]
    logger.info(f"Uploading directory: {dir}")
    logger.info(f"Number of files found: {len(dir_files)}")
    for file in dir_files:
        # dont delete file, just let the backbalze lifetime file settings delete it
        # if you try to delete a file that doesnt exist, it creates an object application/x-bz-hide-marker
        try:
            s3.upload_file(
                str(file), 
                os.environ["B2_BUCKET_NAME"], 
                str(file.relative_to(root).as_posix())
            )
            logger.info(f"Uploaded {file.relative_to(dir)} to Backblaze successfully")
        except Exception as e:
            logger.error(f"Failed to upload {file}: {e}")
            # Making early return in case one file upload fails
            return {'status':'error', 'status_type':'Failed to upload a file from directory', 'data':None}
            
    return {'status':'ok', 'status_type':None, 'data':None}


def commit_file(file:Path, commit_msg, logger):
    try:
        subprocess.run(["git", "add", str(file)], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push"], check=True)

        logger.info(f"Commit successful: {file.name}")
    except Exception as e:
        logger.error(f"Failed to commit {file.name}: {e}")

def update_process_state(process_state, country, process_type, process_status="pending", process_error=None):
    process_state.setdefault(country, {
        key: {"status": "pending", "last_run": None, "error": None} for key in 
        ["scrape", "clean", "test_basic", "test_first_level", "test_duplicates", "fix"]
    })
    process_state[country][process_type]['status'] = process_status
    process_state[country][process_type]['last_run'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    process_state[country][process_type]['error'] = process_error

def donwload_country_data_from_bucket(countries, bucket_dir:Path, save_dir:Path, s3, logger):
    downloaded_count = 0
    to_download_total = 0
    list_obj_response = s3.list_objects_v2(Bucket=os.environ["B2_BUCKET_NAME"], Prefix=bucket_dir.as_posix())
    files_list = [(obj['Key']) for obj in list_obj_response['Contents']]
    logger.info(f"Total files found for bucket in '{bucket_dir}': {len(files_list)}")

    logger.info(f"* Total of countries to download: {len(countries)}")
    # load data from b2 bucket for countries to process
    for count, country in enumerate(countries):
        country_files = [str(file) for file in files_list if re.match(rf"{bucket_dir.as_posix()}/{country}/.+\.json", file)]
        to_download_total += len(country_files)
        logger.info(f"* Country {country} ({count}/{len(countries)}) files found: {len(country_files)}")
        for file in country_files:
            save_file = save_dir / country / os.path.basename(file)
            if save_file.exists():
                logger.info(f"  * Skip existing file {save_file}")
                continue
            
            os.makedirs(save_file.parent, exist_ok=True)
            try:
                s3.download_file(os.environ["B2_BUCKET_NAME"], file, str(save_file))
                logger.info(f"  * File '{file}' downloaded successfully to '{save_file}'")
                downloaded_count += 1
            except Exception as e:
                logger.error(f"  * Error downloading file '{file}': {e}")

    logger.info(f"* Number of downloaded files: {downloaded_count}/{to_download_total}")