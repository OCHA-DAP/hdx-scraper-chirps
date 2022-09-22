import logging
from bs4 import BeautifulSoup
from geopandas import read_file
from mapbox import Uploader
from time import sleep

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.hdxobject import HDXError

logger = logging.getLogger(__name__)


def get_latest_data(base_url, downloader):
    response = downloader.download(base_url)
    if response.status_code != 200:
        logger.error(f"Could not get data from {base_url}")
        return None
    latest_url = None
    soup = BeautifulSoup(downloader.get_text(), "html.parser")
    lines = soup.find_all("a")
    for line in lines:
        filename = line.get("href")
        if filename[-3:] != "zip":
            continue
        if not latest_url:
            latest_url = filename
            continue
        latest_date = int(latest_url.split("/")[-1].split("_")[5])
        filedate = int(filename.split("_")[5])
        if filedate > latest_date:
            latest_url = f"{base_url}{filename}"

    return latest_url


def update_dataset(dataset_name, latest_data):
    logger.info("Updating dataset")

    dataset = Dataset.read_from_hdx(dataset_name)
    resources = dataset.get_resources()
    resource_names = [r["name"] for r in resources]
    new_resources = list()
    for period in latest_data:
        resource_name = latest_data[period].split("/")[-1].split(".")[0]
        if resource_name in resource_names:
            continue
        resource_desc = None
        pentad = resource_name.split("_")[-2][-2:]
        if period == "marmay":
            resource_desc = f"March to May 2022 (Mar pentad 1 thru May Pentad 6) - Average(1981-2010)\n" \
                            f"Pentad: {pentad}"
        if period == "octdec":
            resource_desc = f"October to December 2022 (Oct pentad 1 thru Dec Pentad 6) - Average(1981-2010)\n" \
                            f"Pentad: {pentad}"
        resource_data = {
            "name": resource_name,
            "description": resource_desc,
            "url": latest_data[period],
            "format": "GeoTIFF",
        }
        resource = Resource(resource_data)
        new_resources.append(resource)
    try:
        dataset.add_update_resources(new_resources)
    except HDXError as ex:
        logger.error(f"Resources could not be added. Error: {ex}")

    return dataset


def summarize_data(downloader, url, boundary_dataset, countries, folder):
    path = downloader.download_file(url)
    dataset = Dataset.read_from_hdx(boundary_dataset)
    resources = dataset.get_resources()
    boundaries = {}
    for resource in resources:
        if "polbnda_adm" in resource["name"]:
            _, resource_file = resource.download(folder=folder)
            boundary_lyr = read_file(resource_file)
            boundary_lyr = boundary_lyr[boundary_lyr["alpha_3"] in countries]
            boundaries[resource["name"]] = boundary_lyr

def generate_mapbox_data(path, boundary_dataset, countries, folder):
    dataset = Dataset.read_from_hdx(boundary_dataset)
    resources = dataset.get_resources()
    rendered_rasters = []
    for resource in resources:
        if "wrl_polbnda_int_1m" in resource["name"]:
            _, resource_file = resource.download(folder=folder)
            boundary_lyr = read_file(resource_file)
            boundary_lyr = boundary_lyr[boundary_lyr["alpha_3"] in countries]
    return rendered_rasters


def upload_to_mapbox(mapid, name, file_to_upload, mapbox_key):
    service = Uploader(access_token=mapbox_key)
    with open(file_to_upload, 'rb') as src:
        upload_resp = service.upload(src, mapid, name=name)
    if upload_resp.status_code == 422:
        for i in range(5):
            sleep(5)
            with open(file_to_upload, 'rb') as src:
                upload_resp = service.upload(src, mapid, name=name)
            if upload_resp.status_code != 422:
                break
    if upload_resp.status_code == 422:
        logger.error(f"Could not upload {name}")
        return None
    return mapid
