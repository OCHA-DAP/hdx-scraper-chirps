import pytest

from os.path import join
from hdx.api.configuration import Configuration
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from chirps import *


class TestChirps:
    latest_url = "ea_chirps_seasaccum_anom_marmay_202230_lta.zip"

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        class Download:
            @staticmethod
            def download(url):
                pass

            @staticmethod
            def get_text():
                with open(join(
                    "tests", "fixtures", "africa_east_pentadal_chirps_seasaccum_marmay_anom_lta_downloads_pentadal.text"
                )) as f:
                    contents = f.read()
                return contents

            @staticmethod
            def download_file(url, folder):
                return join(
                    "tests", "fixtures", "ea_chirps_seasaccum_anom_marmay_202230_lta.zip"
                )

        return Download()

    def test_get_latest_data(self, configuration, downloader):
        latest_url = get_latest_data(configuration["base_url"], downloader)
        assert latest_url == configuration["base_url"]+TestChirps.latest_url
