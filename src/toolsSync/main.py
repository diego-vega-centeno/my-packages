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

def donwload_country_data_from_bucket(countries, bucket_name, bucket_dir:Path, save_dir:Path, s3, logger):

    list_obj_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_dir.as_posix())

    objects_list = [(obj['Key']) for obj in list_obj_response['Contents']]
    b2_countries = tgm.deleteDuplicates([Path(obj['Key']).parent.name  for obj in list_obj_response['Contents']])

    logger.info(f"  * Total objects found in B2 '{bucket_dir}': {len(objects_list)}")
    logger.info(f"  * Total countries found in B2 '{bucket_dir}': {len(b2_countries)}")

    logger.info(f"  * Countries to download: {len(countries)}: {countries}")
    countries_in_b2 = tgm.intersection(countries, b2_countries)
    logger.info(f"  * Countries to download found in B2: {len(countries_in_b2)}")
    logger.info(f"  * Countries to download missing in B2: {len(tgm.complement(countries_in_b2, b2_countries))}")

    logger.info(f"  * Downloading data for countries: {len(countries_in_b2)}")
    logger.info(f"  * Downloading directory: '{bucket_dir}' -> '{save_dir}'")

    downloaded_countries = []
    downloaded_files_count = 0
    to_download_total = 0
    # load data from b2 bucket for countries to process
    for count, country in enumerate(countries ,start=1):
        country_files = [str(file) for file in objects_list if re.match(rf"{bucket_dir.as_posix()}/{country}/.+\.json", file)]
        to_download_total += len(country_files)
        logger.info(f"    * ({count}/{len(countries)}) Country {country} files found: {len(country_files)}")
        if len(country_files) < 1:
            continue
        for file in country_files:
            save_file = save_dir / country / os.path.basename(file)
            if save_file.exists():
                logger.info(f"      * Skip existing file {save_file.name}")
                downloaded_files_count += 1
                continue
            
            os.makedirs(save_file.parent, exist_ok=True)
            try:
                s3.download_file(os.environ["B2_BUCKET_NAME"], file, str(save_file))
                logger.info(f"      * File '{os.path.basename(file)}' downloaded successfully to '{save_file.name}'")
                downloaded_files_count += 1
            except Exception as e:
                logger.error(f"      * Error downloading file '{os.path.basename(file)}': {e}")
        downloaded_countries.append(country)

    logger.info(f"  * Number of downloaded files: {downloaded_files_count}/{to_download_total}")
    return downloaded_countries