import argparse
import logging
from os import getenv
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from chirps import *

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-chirps"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument("-mk", "--mapbox_key", default=None, help="Credentials for accessing MapBox data")
    args = parser.parse_args()
    return args


def main(
    mapbox_key,
    **ignore,
):
    configuration = Configuration.read()
    countries = [c for c in configuration["output"]["mapbox"]]

    with temp_dir(folder="TempCHIRPS") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            seasons = [s for s in configuration["base_url"]]

            logger.info("Finding latest available data")
            latest_data = dict()
            for season in seasons:
                latest_data[season] = get_latest_data(
                    configuration["base_url"][season],
                    downloader,
                    configuration.get("exclude_year"),
                )
            if len(latest_data) == 0:
                return

            logger.info("Generating tif resources")
            dataset = Dataset.read_from_hdx(configuration["output"]["dataset"])
            dataset, updated = add_chirps_to_dataset(dataset,
                                                     latest_data,
                                                     configuration["output"]["resource_desc"])

            if not updated:
                logger.info("No new data found, will try again tomorrow")
                return

            logger.info("Downloading international boundaries")
            boundary_dataset = Dataset.read_from_hdx(configuration["boundaries"]["dataset"])
            boundary_resources = boundary_dataset.get_resources()
            subn_resources = []
            nat_resource = None
            for resource in boundary_resources:
                if "polbnda_adm" in resource["name"]:
                    _, resource_file = resource.download(folder=temp_folder)
                    subn_resources.append(resource_file)
                if "wrl_polbnda_int_1m" in resource["name"]:
                    _, resource_file = resource.download(folder=temp_folder)
                    nat_resource = resource_file

            logger.info("Summarizing data subnationally")
            rasters, zstats = summarize_data(
                downloader,
                latest_data,
                subn_resources,
                countries,
                temp_folder,
            )

            logger.info("Updating HDX")
            csv_path = join(temp_folder, "subnational_anomaly_statistics.csv")
            zstats.to_csv(csv_path, index=False)
            resources = dataset.get_resources()
            for resource in resources:
                if resource.get_file_type() == "csv":
                    resource.set_file_to_upload(csv_path)
            if dataset:
                dataset.update_in_hdx(
                    hxl_update=False,
                    updated_by_script="HDX Scraper: CHIRPS",
                )

            logger.info("Preparing rasters for mapbox")
            rendered_rasters = generate_mapbox_data(
                rasters,
                nat_resource,
                countries,
                configuration["legend"],
                temp_folder,
            )

            logger.info("Uploading rasters to mapbox")
            for country in rendered_rasters:
                for season in rendered_rasters[country]:
                    upload_to_mapbox(
                        configuration["output"]["mapbox"][country][season]["mapid"],
                        configuration["output"]["mapbox"][country][season]["name"],
                        rendered_rasters[country][season],
                        mapbox_key,
                    )


if __name__ == "__main__":
    args = parse_args()
    mapbox_key = args.mapbox_key
    if mapbox_key is None:
        mapbox_key = getenv("MAPBOX_KEY", None)
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        mapbox_key=mapbox_key,
    )
