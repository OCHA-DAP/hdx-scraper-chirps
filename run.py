import argparse
import logging
from os import getenv
from os.path import join, expanduser

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from chirps import *

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-chirps"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-hk", "--hdx_key", default=None, help="HDX api key")
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument("-hs", "--hdx_site", default=None, help="HDX site to use")
    parser.add_argument("-ma", "--mapbox_auth", default=None, help="Credentials for accessing MapBox data")
    args = parser.parse_args()
    return args


def main(**ignore):
    configuration = Configuration.read()
    countries = [c for c in configuration["output"]["mapbox"]]

    with temp_dir(folder="TempCHIRPS") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            logger.info("Finding latest available data")
            base_urls = configuration["base_url"]
            latest_data = dict()
            for period in base_urls:
                link = get_latest_data(base_urls[period], downloader)
                latest_data[period] = link

            logger.info("Generating resources")
            dataset = update_dataset(configuration["output"]["dataset"], latest_data)
            # if dataset:
            #     dataset.update_in_hdx(
            #         hxl_update=False,
            #         updated_by_script="HDX Scraper: CHIRPS",
            #     )

            logger.info("Summarizing data subnationally")
            dataset = summarize_data(
                downloader,
                latest_data[period],
                configuration["output"]["dataset"],
                countries,
                temp_folder,
            )
            if dataset:
                dataset.update_in_hdx(
                    hxl_update=False,
                    updated_by_script="HDX Scraper: CHIRPS",
                )

            logger.info("Preparing rasters for mapbox")
            rasters = generate_mapbox_data(
                downloader,
                latest_data[period],
                configuration["boundaries"]["dataset"],
                temp_folder,
            )

            logger.info("Uploading rasters to mapbox")
            for country in rasters:
                upload_to_mapbox(
                    configuration["output"]["mapbox"][country]["mapid"],
                    configuration["output"]["mapbox"][country]["name"],
                    rasters[country],
                    mapbox_auth,
                )


if __name__ == "__main__":
    args = parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    mapbox_auth = args.mapbox_auth
    if mapbox_auth is None:
        mapbox_auth = getenv("MAPBOX_AUTH", None)
    facade(
        main,
        hdx_key=hdx_key,
        hdx_site="prod",
        user_agent=user_agent,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        preprefix=preprefix,
        project_config_yaml=join("config", "project_configuration.yml"),
        mapbox_auth=mapbox_auth,
    )
