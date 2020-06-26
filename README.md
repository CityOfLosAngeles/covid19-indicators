covid19-indicators
==============================

Key COVID-19 and public health indicators for reopening

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── Dockerfile               <- Docker image for this project.
    ├── data                     <- Scripts to create the data.
    ├── catalog                  <- Catalog listing data sources from open data portals.
    ├── notebooks                <- Jupyter notebooks.
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment,
    │                               e.g generated with `pip freeze > requirements.txt`
    ├── main.py                  <- Used to send our daily pdf reports by email. 
    ├── report.py                <- Used to automate writing the daily report on GitHub pages. 
    ├── setup.py                 <- Makes project pip installable (pip install -e .) 



--------

This repository will track COVID-19 indicators as LA considers its reopening strategy. We will also provide sample notebooks for how others can use the Johns Hopkins University COVID-19 data, which is available for all US counties, to look at trends in other counties or states. 

**DAILY REPORT:** [https://cityoflosangeles.github.io/covid19-indicators/coronavirus-stats.html](https://cityoflosangeles.github.io/covid19-indicators/coronavirus-stats.html)

The City of LA uses [US county data](https://www.arcgis.com/home/item.html?id=628578697fb24d8ea4c32fa0c5ae1843) published by [JHU](https://www.esri.com/arcgis-blog/products/product/public-safety/coronavirus-covid-19-data-available-by-county-from-johns-hopkins-university/). The historical time-series is pulled from JHU's CSV on GitHub and appended with the current date's data from the ESRI feature layer.

Our ESRI data sources are public and listed below. The full documentation of the City of LA's COVID-19 data and scripts is available in our [Aqueduct COVID-19 ETL work's README](https://github.com/CityOfLosAngeles/aqueduct/tree/master/dags/public-health/covid19/README.md). We are in progress of moving these data sources off of ESRI and moving to an S3 bucket. See our work in the `data` folder.


1. [Data Sources](#data-sources)
1. [Helpful Hints for Jupyter Notebooks](#helpful-hints)
1. [Setting up a Conda Environment](#setting-up-a-conda-environment)
1. [Starting with Docker](#starting-with-docker)
1. [Emailing the Report](#emailing-the-report)

## Data Sources
* Scripts to ingest, process, and save our data sources are in the `data` folder.
* CA Department of Public Health: [COVID-related open data](https://data.ca.gov/dataset?q=covid&sort=score+desc%2C+metadata_modified+desc)
    * `catalog.yml` lists the datasets pulled from the state's open data portal 

#### COVID-19 Cases
* Global province-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=daeef8efe43941748cb98d7c1f716122)

* Global province-level current date's [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=191df200230642099002039816dc8c59) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=6f3f214220f443b2beed8d1374b02cf7)

* US county-level time-series [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=8f13bb3abefe490f9edd47df89664b56) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=782ca660304a4bdda1cc9757a2504647)

* Comparison of metropolitan infection rates [feature table](http://lahub.maps.arcgis.com/home/item.html?id=b37e229b71dc4c65a479e4b5912ded66) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=27efb06ce2954b90ae833dabb570b1cf). The [MSA to county crosswalk](https://github.com/CityOfLosAngeles/aqueduct/blob/master/dags/public-health/covid19/msa_county_pop_crosswalk.csv) was derived from the [National Bureau of Economic Research crosswalk](https://data.nber.org/data/cbsa-msa-fips-ssa-county-crosswalk.html) using `make-crosswalk.py`.

* LA County Dept of Public Health (DPH) neighborhood-level current date's [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=ca30397902484e9c911e8092788a0233), and the City's GeoHub [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=9681f6a5269f4b6997b398bca057b62c) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=f553fa74cbfe4e49b3a334947298fa34)

* City of LA case count time-series [feature table](https://lahub.maps.arcgis.com/home/item.html?id=1d1e4679a94e43e884b97a0488fc04cf) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=7175fba373f541a7a19df56b6a0617f4)

#### Hospital Bed and Equipment Availability
* LA County hospital bed and equipment availability [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=956e105f422a4c1ba9ce5d215b835951) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=3da1eb3e13a14743973c96b945bd1117). Data is available for the [70 largest hospitals](http://file.lacounty.gov/SDSInter/dhs/1070069_HavBedSummary.pdf) in the county and collected in the HavBed survey.

#### COVID-19 Testing
* LA County COVID-19 tests administered [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=64b91665fef4471dafb6b2ff98daee6c) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=158dab4a07b04ecb8d47fea1746303ac)

* LA County and LA City COVID-19 tests administered [feature layer](https://lahub.maps.arcgis.com/home/item.html?id=996a863e59f04efdbe33206a6c717afb) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=3cfd003985b447c994a7252e8eb97b92)

* The City of LA tests are a subset of LA County tests. Tests done in other parts of LA County (excluding the City of LA) would be derived by taking the difference of `Performed` - `City_Performed`.


## Helpful Hints
Jupyter Notebooks can read in both the ESRI feature layer and the CSV. In our [Data Sources](#data-sources), we often provide links to the ESRI feature layer and CSV. More in our [COVID-19 indicators GitHub repo](https://github.com/CityOfLosAngeles/covid19-indicators).

Ex: JHU global province-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=daeef8efe43941748cb98d7c1f716122)

**Import the CSV**

All you need is the item ID of the CSV item. We use an f-string to construct the URL and use Python `pandas` package to import the CSV.

```
JHU_GLOBAL_ITEM_ID = "daeef8efe43941748cb98d7c1f716122"

JHU_URL = f"http://lahub.maps.arcgis.com/sharing/rest/content/items/{JHU_GLOBAL_ITEM_ID}/data"

import pandas as pd
df = pd.read_csv(JHU_URL)
```

**Import ESRI feature layer**

* From the feature layer, click on `service URL`.
* Scroll to the bottom and click `Query`
* Fill in the following parameters:
    * WHERE: 1=1
    * Out Fields (fill in the list of columns to retrieve): Province_State, Country_Region, Lat, Long, date, number_of_cases, number_of_deaths, number_of_recovered, ObjectId
    * Format: GeoJSON
    * Query (GET)
* Now, grab the new URL (it should be quite long), and read in that URL through `geopandas`. Note: the ESRI date field is less understandable, and converting it to pandas datetime will be incorrect.

```
FEATURE_LAYER_URL = "http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580"

SERVICE_URL = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/jhu_covid19_time_series/FeatureServer/0"

CORRECT_URL = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/jhu_covid19_time_series/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Province_State%2C+Country_Region%2C+Lat%2C+Long%2C+date%2C+number_of_cases%2C+number_of_deaths%2C+number_of_recovered%2C+ObjectId&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="


import geopandas as gpd
gdf = gpd.read_file(CORRECT_URL)
```

To convert to HTML: `jupyter nbconvert --to html --no-input --no-prompt my-notebook.ipynb`


## Setting up a Conda Environment 
1. `conda create --name my_project_name` 
1. `source activate my_project_name`
1. `conda install --file conda-requirements.txt -c conda-forge` 
1. `pip install requirements.txt`

## Starting with Docker
1. Start with Steps 1-2 above
1. Build Docker container: `docker-compose.exe build`
1. Start Docker container `docker-compose.exe up`
1. Open Jupyter Lab notebook by typing `localhost:8888/lab/` in the browser.

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


## Emailing the Report 
To setup the report for daily emailing, you'll need to have AWS SES configured and setup on your account.

1. `docker-compose build`
1. `docker-compose run lab python /app/main.py` 