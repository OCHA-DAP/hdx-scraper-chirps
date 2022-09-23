import logging

from bs4 import BeautifulSoup
from geopandas import read_file
from mapbox import Uploader
from numpy import zeros
from os.path import join
from pandas import concat
from rasterio import mask
from rasterio import open as r_open
from rasterio.dtypes import uint8
from rasterio.enums import Resampling
from rasterstats import zonal_stats
from time import sleep
from zipfile import ZipFile

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


def add_chirps_to_dataset(dataset_name, latest_data):
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
    if len(new_resources) == 0:
        return dataset
    try:
        dataset.add_update_resources(new_resources)
    except HDXError as ex:
        logger.error(f"Resources could not be added. Error: {ex}")
    return dataset


def summarize_data(downloader, url, boundary_dataset, dataset, countries, folder):
    path = downloader.download_file(url, folder=folder)
    with ZipFile(path, "r") as z:
        rastername = z.namelist()
        z.extractall(path=folder)
    try:
        raster = join(folder, rastername[0])
    except IndexError:
        logger.error("Could not extract CHIRPS data")
        return None, None
    b_dataset = Dataset.read_from_hdx(boundary_dataset)
    resources = b_dataset.get_resources()
    zstats = []
    for resource in resources:
        if "polbnda_adm" in resource["name"]:
            level = resource["name"][11]
            _, resource_file = resource.download(folder=folder)
            boundary_lyr = read_file(resource_file)
            boundary_lyr = boundary_lyr[boundary_lyr["alpha_3"].isin(countries)]
            boundary_lyr["ADM_LEVEL"] = int(level)
            boundary_lyr["ADM_PCODE"] = boundary_lyr[f"ADM{level}_PCODE"]
            boundary_lyr["ADM_REF"] = boundary_lyr[f"ADM{level}_REF"]
            stats = zonal_stats(
                vectors=boundary_lyr,
                raster=raster,
                stats=["mean", "min", "max"],
                geojson_out=True,
            )
            for row in stats:
                pcode = row["properties"][f"ADM{level}_PCODE"]
                boundary_lyr.loc[
                    boundary_lyr[f"ADM{level}_PCODE"] == pcode, "CHIRPS_mean"
                ] = row["properties"]["mean"]
                boundary_lyr.loc[
                    boundary_lyr[f"ADM{level}_PCODE"] == pcode, "CHIRPS_min"
                ] = row["properties"]["min"]
                boundary_lyr.loc[
                    boundary_lyr[f"ADM{level}_PCODE"] == pcode, "CHIRPS_max"
                ] = row["properties"]["max"]
            zstats.append(boundary_lyr)
    zstats = concat(zstats)
    zstats.drop(columns="geometry", inplace=True)
    zstats.to_csv(join(folder, "subnational_anomaly_statistics.csv"), index=False)
    resources = dataset.get_resources()
    for resource in resources:
        if resource.get_file_type() == "csv":
            resource.set_file_to_upload(join(folder, "subnational_anomaly_statistics.csv"))
    return raster, dataset


def generate_mapbox_data(raster, boundary_dataset, countries, legend, folder):
    dataset = Dataset.read_from_hdx(boundary_dataset)
    resources = dataset.get_resources()
    rendered_rasters = dict()
    boundary_lyr = None
    for resource in resources:
        if "wrl_polbnda_int_1m" in resource["name"]:
            _, resource_file = resource.download(folder=folder)
            boundary_lyr = read_file(resource_file)
    open_raster = r_open(raster)
    for country in countries:
        clip_raster = join(folder, f"{country}_clip.tif")
        resample_raster = join(folder, f"{country}_resample.tif")
        render_raster = join(folder, f"{country}_render.tif")
        meta = open_raster.meta

        # Clip raster to country outline
        country_geom = boundary_lyr["geometry"][boundary_lyr["ISO_3"] == country]
        clipped, transform = mask.mask(open_raster, country_geom, all_touched=True, crop=True)
        meta.update({"height": clipped.shape[1],
                     "width": clipped.shape[2],
                     "transform": transform})
        with r_open(clip_raster, "w", **meta) as dst:
            dst.write(clipped)

        # Resample raster to increase resolution
        with r_open(clip_raster) as src:
            upscale_factor = int(round(3000/src.width, 0))
            data = src.read(
                out_shape=(src.count, int(src.height * upscale_factor), int(src.width * upscale_factor)),
                resampling=Resampling.nearest
            )
            transform = src.transform * src.transform.scale(
                (src.width / data.shape[-1]),
                (src.height / data.shape[-2])
            )
        meta.update({"height": data.shape[1],
                     "width": data.shape[2],
                     "transform": transform})
        with r_open(resample_raster, "w", **meta) as dst:
            dst.write(data)

        # Render as raster with red, green, blue, alpha bands
        color_bands = [zeros(shape=clipped.shape, dtype=uint8),
                       zeros(shape=clipped.shape, dtype=uint8),
                       zeros(shape=clipped.shape, dtype=uint8),
                       zeros(shape=clipped.shape, dtype=uint8)]
        for color in legend:
            color_bands[0][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][0]
            color_bands[1][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][1]
            color_bands[2][(clipped > color["range"][0]) & (clipped <= color["range"][1])] = color["color"][2]
        color_bands[3][clipped > -100000] = 255
        meta.update({"count": 4,
                     "dtype": 'uint8',
                     "nodata": None})
        with r_open(render_raster, "w", **meta) as final:
            meta.update({"count": 1})
            for i, c in enumerate(color_bands, start=1):
                color_raster = join(folder, f"{country}_color.tif")
                with r_open(color_raster, "w", **meta) as dst:
                    dst.write(c)
                with r_open(color_raster) as src:
                    final.write_band(i, src.read(1))
        rendered_rasters[country] = render_raster
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
