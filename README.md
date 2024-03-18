### Collector for CHIRPS dataset
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-chirps/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-chirps/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-chirps/badge.svg?branch=main)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-chirps?branch=main)

This script collects the latest CHIRPS anomaly data from the [USGS site](https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/pentadal/chirps/seasaccum/) and adds to an HDX dataset. It then summarizes the data by subnational unit and updates a tabular resource in that dataset.

### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-chirps** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: HDX_SITE, HDX_KEY, USER_AGENT, PREPREFIX
