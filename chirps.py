import logging
import warnings

from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
from geopandas import read_file
from os.path import basename, join
from pandas import DataFrame, concat
from rasterstats import zonal_stats
from shapely.errors import ShapelyDeprecationWarning
from zipfile import ZipFile

from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.base_downloader import DownloadError

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def get_latest_data(base_url, downloader, exclude_year=None):
    try:
        downloader.download(base_url)
    except DownloadError:
        logger.error(f"Could not get data from {base_url}")
        return None
    latest_file = None
    soup = BeautifulSoup(downloader.get_text(), "html.parser")
    lines = soup.find_all("a")
    for line in lines:
        filename = line.get("href")
        if filename[-3:] != "zip":
            continue
        fileyear = filename.split("_")[5][:4]
        if exclude_year and fileyear == str(exclude_year):
            continue
        if not latest_file:
            latest_file = filename
            continue
        latest_date = int(latest_file.split("_")[5])
        filedate = int(filename.split("_")[5])
        if filedate > latest_date:
            latest_file = filename
    latest_url = f"{base_url}{latest_file}"

    return latest_url


def add_chirps_to_dataset(dataset, latest_data, resource_desc):
    updated = []
    resources = [r for r in dataset.get_resources() if r.get_file_type() == "geotiff"]
    for season in latest_data:
        matching_resources = [r for r in resources if season in r["name"]]
        matching_index = [i for i, _ in enumerate(resources) if season in resources[i]["name"]]
        resource_name = latest_data[season].split("/")[-1].split(".")[0]
        if resource_name in [r["name"] for r in matching_resources]:
            updated.append(False)
            continue
        pentad = resource_name.split("_")[-2][-2:]
        resource_year = resource_name.split("_")[5][:4]
        desc = resource_desc[season].replace("YYYY", resource_year) + pentad

        if len(matching_resources) == 0:
            updated.append(True)
            resource_data = {
                "name": resource_name,
                "description": desc,
                "url": latest_data[season],
                "format": "GeoTIFF",
            }
            resources.append(Resource(resource_data))
        elif matching_resources[0]["url"] != latest_data[season]:
            updated.append(True)
            resources[matching_index[0]]["name"] = resource_name
            resources[matching_index[0]]["description"] = desc
            resources[matching_index[0]]["url"] = latest_data[season]

    updated = any(updated)
    if not updated:
        return dataset, updated

    start_dates = []
    end_dates = []
    for resource_name in [r["name"] for r in resources]:
        pentad = resource_name.split("_")[-2][-2:]
        year = int(resource_name.split("_")[-2][:-2])
        month = (int(pentad) - 1) // 6 + 1
        start_day = ((int(pentad) - 1) % 6) * 5 + 1
        start_date = datetime(year, month, start_day)
        if start_day < 25:
            end_date = datetime(year, month, start_day + 4)
        else:  # last week of the month
            end_date = start_date + relativedelta(day=31)
        start_dates.append(start_date)
        end_dates.append(end_date)
    dataset.set_reference_period(startdate=min(start_dates), enddate=max(end_dates))

    try:
        dataset.add_update_resources(resources, ignore_datasetid=True)
    except HDXError as ex:
        updated = False
        logger.error(f"Resources could not be added. Error: {ex}")

    return dataset, updated


def summarize_data(downloader, latest_data, subn_resources, countries, folder):
    rasters = dict()
    zstats = []
    for season in latest_data:
        path = downloader.download_file(latest_data[season], folder=folder)
        with ZipFile(path, "r") as z:
            rastername = z.namelist()
            z.extractall(path=folder)
        try:
            raster = join(folder, rastername[0])
        except IndexError:
            logger.error("Could not extract CHIRPS data")
            return None, None
        rasters[season] = raster
        for resource in subn_resources:
            level = basename(resource)[11]
            boundary_lyr = read_file(resource)
            boundary_lyr = boundary_lyr[boundary_lyr["alpha_3"].isin(countries)]
            boundary_lyr["ADM_LEVEL"] = int(level)
            boundary_lyr["ADM_PCODE"] = boundary_lyr[f"ADM{level}_PCODE"]
            boundary_lyr["ADM_REF"] = boundary_lyr[f"ADM{level}_REF"]
            boundary_lyr.sort_values(by=["ADM_PCODE"], inplace=True)
            boundary_lyr["Season"] = season
            stats = zonal_stats(
                vectors=boundary_lyr,
                raster=raster,
                stats=["mean", "min", "max"],
                geojson_out=True,
            )
            for row in stats:
                pcode = row["properties"]["ADM_PCODE"]
                if row["properties"]["mean"]:
                    boundary_lyr.loc[boundary_lyr["ADM_PCODE"] == pcode, "CHIRPS_mean"] = round(
                        row["properties"]["mean"], 5
                    )
                if row["properties"]["min"]:
                    boundary_lyr.loc[boundary_lyr["ADM_PCODE"] == pcode, "CHIRPS_min"] = round(
                        row["properties"]["min"], 5
                    )
                if row["properties"]["max"]:
                    boundary_lyr.loc[boundary_lyr["ADM_PCODE"] == pcode, "CHIRPS_max"] = round(
                        row["properties"]["max"], 5
                    )
            zstats.append(boundary_lyr)
    zstats = concat(zstats)
    zstats = DataFrame(zstats.drop(columns="geometry").reset_index(drop=True))

    return rasters, zstats

