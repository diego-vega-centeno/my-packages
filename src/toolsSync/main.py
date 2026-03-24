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
            logger.info(f"Uploading {file.relative_to(dir)} ...")
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

def upload_file_to_backblaze(file:Path, config):
    s3 = config['s3']
    logger = config['logger']
    root = config['root']
    try:
        s3.upload_file(
            str(file), 
            os.environ["B2_BUCKET_NAME"], 
            str(file.relative_to(root).as_posix())
        )
        logger.info(f"Uploaded {file} to Backblaze successfully")
        return {'status':'ok', 'status_type':None, 'data':None}
    except Exception as e:
        logger.error(f"Failed to upload {file}: {e}")
        return {'status':'error', 'status_type':'Failed to upload a file from directory', 'data':None}
    
def commit_file(file:Path, commit_msg, logger):
    try:
        subprocess.run(["git", "add", str(file)], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push", "origin", "main"], check=True)
            logger.info(f"Commit successful: {file.name}")
        else:
            logger.info(f"No changes to commit for: {file.name}")
    except Exception as e:
        logger.error(f"Failed to commit {file.name}: {e}")
    finally:
        # Always go back to main
        subprocess.run(["git", "checkout", "main"])

def update_process_state(process_state, country, process_type, process_status="pending", process_error=None):
    process_state.setdefault(country, {
        key: {"status": "pending", "last_run": None, "error": None} for key in 
        ["scrape", "clean", "test_basic", "test_first_level", "test_duplicates", "fix"]
    })
    process_state[country][process_type]['status'] = process_status
    process_state[country][process_type]['last_run'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    process_state[country][process_type]['error'] = process_error

def donwload_country_data_from_bucket(countries, bucket_name, bucket_dir:Path, save_dir:Path, s3, logger):

    # list_obj_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_dir.as_posix())
    list_obj_response_contents = get_bucket_contents(s3, bucket_name, bucket_dir.as_posix())

    objects_list = [(obj['Key']) for obj in list_obj_response_contents]
    all_countries_in_b2 = tgm.delete_duplicates([Path(obj['Key']).parent.name  for obj in list_obj_response_contents])

    logger.info(f"  * Countries to get data: {len(countries)}")
    to_download_countries_in_b2 = tgm.intersection(countries, all_countries_in_b2)
    logger.info(f"    * Found in B2: {len(to_download_countries_in_b2)}, Missing in B2: {len(tgm.complement(countries, all_countries_in_b2))}")

    logger.info(f"  * Downloading data for countries: {len(to_download_countries_in_b2)}")
    logger.info(f"    * Directory: '{bucket_dir}' -> '{save_dir}'")

    downloaded_countries = []
    downloaded_files_count = 0
    to_download_total = 0
    # load data from b2 bucket for countries to process
    for count, country in enumerate(to_download_countries_in_b2 ,start=1):
        country_files = [str(file) for file in objects_list if re.match(rf"{bucket_dir.as_posix()}/{country}/.+", file)]
        to_download_total += len(country_files)
        logger.info(f"    * ({count}/{len(to_download_countries_in_b2)}) Country {country} files found: {len(country_files)}")
        if len(country_files) < 1:
            continue
        for file in country_files:
            save_file = save_dir / country / os.path.basename(file)
            if save_file.exists():
                logger.info(f"      * Skip download of existing file {save_file.name}")
                downloaded_files_count += 1
                continue
            
            os.makedirs(save_file.parent, exist_ok=True)
            try:
                s3.download_file(bucket_name, file, str(save_file))
                logger.info(f"      * File '{os.path.basename(file)}' downloaded successfully to '{save_file.name}'")
                downloaded_files_count += 1
            except Exception as e:
                logger.error(f"      * Error downloading file '{os.path.basename(file)}': {e}")
        downloaded_countries.append(country)

    logger.info(f"  * Number of downloaded files: {downloaded_files_count}/{to_download_total}")
    return downloaded_countries

def download_file_from_bucket(bucket_name, file:Path, s3, save_dir:Path, logger):
    os.makedirs(os.path.dirname(save_dir), exist_ok=True)
    try:
        s3.download_file(bucket_name, str(file.as_posix()), str(save_dir))
    except Exception as e:
        logger.error(f"Failed to donwload {file}")
        logger.error(e)


def get_bucket_contents(s3, bucket_name, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    all_objects = []
    for page in pages:
        all_objects.extend(page.get("Contents", []))
    return all_objects