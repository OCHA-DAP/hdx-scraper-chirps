from os.path import join
from pandas import read_csv, testing
from rasterio import open as r_open

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from chirps import *


class TestChirps:
    latest_url = "https://lala/ea_chirps_seasaccum_anom_marmay_202230_lta.zip"
    countries = ["SOM"]
    subn_resources = [join("tests", "fixtures", "polbnda_adm1_1m_ocha.geojson")]
    nat_resource = join("tests", "fixtures", "wrl_polbnda_int_1m_uncs.geojson")
    zstats = read_csv(join("tests", "fixtures", "subnational_anomaly_statistics.csv"))
    raster = join("tests", "fixtures", "ea_chirps_seasaccum_anom_marmay_202230_lta.tif")
    rendered_raster = r_open(join("tests", "fixtures", "SOM_render.tif")).read()

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
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
        latest_url = get_latest_data("https://lala/", downloader)
        assert latest_url == TestChirps.latest_url

    def test_add_chirps_to_dataset(self, configuration, downloader):
        dataset = Dataset.load_from_json(join(
            "tests", "fixtures", "east-africa-chirps-seasonal-rainfall-accumulation-anomaly-by-pentad.json"
        ))
        dataset, updated = add_chirps_to_dataset(
            dataset,
            TestChirps.latest_url,
            "March to May 2022 (Mar pentad 1 thru May Pentad 6) - Average(1981-2010)\nPentad: ")
        dataset.get_date_of_dataset()
        assert dataset["dataset_date"] == "[2022-05-25T00:00:00 TO 2022-05-31T00:00:00]"
        assert updated is True
        assert dataset.get_resources()[0] == {
            "name": "ea_chirps_seasaccum_anom_marmay_202230_lta",
            "description": "March to May 2022 (Mar pentad 1 thru May Pentad 6) - Average(1981-2010)\nPentad: 30",
            "url": "https://lala/ea_chirps_seasaccum_anom_marmay_202230_lta.zip",
            "format": "geotiff",
            "resource_type": "api",
            "url_type": "api"
        }

    def test_summarize_data(self, downloader):
        with temp_dir("TestCHIRPS", delete_on_success=True, delete_on_failure=False) as temp_folder:
            raster, zstats = summarize_data(
                downloader,
                TestChirps.latest_url,
                TestChirps.subn_resources,
                TestChirps.countries,
                temp_folder,
            )
            assert raster == join(temp_folder, "ea_chirps_seasaccum_anom_marmay_202230_lta.tif")
            testing.assert_frame_equal(zstats, TestChirps.zstats, check_like=True, check_names=False)

    def test_generate_mapbox_data(self, configuration):
        with temp_dir("TestCHIRPS", delete_on_success=True, delete_on_failure=False) as temp_folder:
            rasters = generate_mapbox_data(
                TestChirps.raster,
                TestChirps.nat_resource,
                TestChirps.countries,
                configuration["legend"],
                temp_folder,
            )
            rendered_raster = r_open(rasters[TestChirps.countries[0]]).read()
            assert (rendered_raster == TestChirps.rendered_raster).all()
