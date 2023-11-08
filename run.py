import logging
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from chirps import *

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-chirps"


def main(**ignore):
    configuration = Configuration.read()
    countries = ["ETH", "KEN", "SOM"]

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
            boundary_dataset = Dataset.read_from_hdx(configuration["boundaries"])
            boundary_resources = boundary_dataset.get_resources()
            subn_resources = []
            for resource in boundary_resources:
                if "polbnda_adm" in resource["name"]:
                    _, resource_file = resource.download(folder=temp_folder)
                    subn_resources.append(resource_file)

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


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
