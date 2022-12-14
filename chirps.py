import logging

from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
from geopandas import read_file
from mapbox import Uploader
from numpy import zeros
from os.path import basename, join
from pandas import DataFrame, concat
from rasterio import mask
from rasterio import open as r_open
from rasterio.dtypes import uint8
from rasterio.enums import Resampling
from rasterstats import zonal_stats
from time import sleep
from zipfile import ZipFile

from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.base_downloader import DownloadError

logger = logging.getLogger(__name__)


def get_latest_data(base_url, downloader):
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
        if not latest_file:
            latest_file = filename
            continue
        latest_date = int(latest_file.split("_")[5])
        filedate = int(filename.split("_")[5])
        if filedate > latest_date:
            latest_file = filename
    latest_url = f"{base_url}{filename}"

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
        desc = resource_desc[season] + pentad

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
    dataset.set_date_of_dataset(startdate=min(start_dates), enddate=max(end_dates))

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


def generate_mapbox_data(rasters, boundary_file, countries, legend, folder):
    rendered_rasters = dict()
    boundary_lyr = read_file(boundary_file)
    for season in rasters:
        open_raster = r_open(rasters[season])
        for country in countries:
            clip_raster = join(folder, f"{country}_{season}_clip.tif")
            resample_raster = join(folder, f"{country}_{season}_resample.tif")
            render_raster = join(folder, f"{country}_{season}_render.tif")
            meta = open_raster.meta

            # Clip raster to country outline
            country_geom = boundary_lyr["geometry"][boundary_lyr["ISO_3"] == country]
            clipped, transform = mask.mask(open_raster, country_geom, all_touched=True, crop=True)
            meta.update({
                "height": clipped.shape[1],
                "width": clipped.shape[2],
                "transform": transform,
            })
            with r_open(clip_raster, "w", **meta) as dst:
                dst.write(clipped)

            # Resample raster to increase resolution
            with r_open(clip_raster) as src:
                upscale_factor = int(round(3000 / src.width, 0))
                data = src.read(
                    out_shape=(src.count, int(src.height * upscale_factor), int(src.width * upscale_factor)),
                    resampling=Resampling.nearest,
                )
                transform = src.transform * src.transform.scale(
                    (src.width / data.shape[-1]), (src.height / data.shape[-2])
                )
            meta.update({"height": data.shape[1], "width": data.shape[2], "transform": transform})
            with r_open(resample_raster, "w", **meta) as dst:
                dst.write(data)

            # Render as raster with red, green, blue, alpha bands
            color_bands = [
                zeros(shape=clipped.shape, dtype=uint8),
                zeros(shape=clipped.shape, dtype=uint8),
                zeros(shape=clipped.shape, dtype=uint8),
                zeros(shape=clipped.shape, dtype=uint8),
            ]
            for color in legend:
                color_bands[0][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][0]
                color_bands[1][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][1]
                color_bands[2][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][2]
            color_bands[3][clipped > -100000] = 255
            meta.update({"count": 4, "dtype": "uint8", "nodata": None})
            with r_open(render_raster, "w", **meta) as final:
                meta.update({"count": 1})
                for i, c in enumerate(color_bands, start=1):
                    color_raster = join(folder, f"{country}_{season}_color.tif")
                    with r_open(color_raster, "w", **meta) as dst:
                        dst.write(c)
                    with r_open(color_raster) as src:
                        final.write_band(i, src.read(1))
            rendered_rasters[country] = {season: render_raster}
    return rendered_rasters


def upload_to_mapbox(mapid, name, file_to_upload, mapbox_key):
    service = Uploader(access_token=mapbox_key)
    with open(file_to_upload, "rb") as src:
        upload_resp = service.upload(src, mapid, name=name)
    if upload_resp.status_code == 422:
        for i in range(5):
            sleep(5)
            with open(file_to_upload, "rb") as src:
                upload_resp = service.upload(src, mapid, name=name)
            if upload_resp.status_code != 422:
                break
    if upload_resp.status_code == 422:
        logger.error(f"Could not upload {name}")
        return None
    return mapid
