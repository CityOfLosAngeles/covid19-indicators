covid19-indicators
==============================

Key COVID-19 and public health indicators for reopening

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── Dockerfile               <- Docker image for this project.
    ├── data                     <- Scripts to create the data and CSVs.
    ├── catalog                  <- Catalog listing data sources used.
    ├── notebooks                <- Jupyter notebooks.
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment,
    │                               e.g generated with `pip freeze > requirements.txt`
    ├── main.py                  <- Used to send our daily pdf reports by email. 
    ├── report.py                <- Used to automate writing the daily report on GitHub pages. 
    ├── setup.py                 <- Makes project pip installable (pip install -e .) 



--------

This repository will track COVID-19 indicators as LA considers its reopening strategy. We will also provide sample notebooks for how others can use the Johns Hopkins University COVID-19 data, which is available for all US counties, to look at trends in other counties or states. *Related repo*: [https://github.com/CityOfLosAngeles/covid19-rmarkdown](https://github.com/CityOfLosAngeles/covid19-rmarkdown)

**LA COUNTY DETAILED DAILY REPORT:** [https://cityoflosangeles.github.io/covid19-indicators/coronavirus-stats.html](https://cityoflosangeles.github.io/covid19-indicators/coronavirus-stats.html)

**CA COUNTIES REPORT:** [https://tinyurl.com/cacovidtrends](https://cityoflosangeles.github.io/covid19-indicators/ca-county-trends.html)

**OTHER MAJOR US COUNTIES REPORT:** [https://tinyurl.com/uscountycovidtrends](https://cityoflosangeles.github.io/covid19-indicators/us-county-trends.html)

**LA COUNTY NEIGHBORHOODS REPORT:** [https://tinyurl.com/laneighborhoodcovidtrends](https://cityoflosangeles.github.io/covid19-indicators/la-neighborhoods-trends.html)

The City of LA uses [US county data](https://www.arcgis.com/home/item.html?id=628578697fb24d8ea4c32fa0c5ae1843) published by [JHU](https://www.esri.com/arcgis-blog/products/product/public-safety/coronavirus-covid-19-data-available-by-county-from-johns-hopkins-university/). The historical time-series is pulled from JHU's CSV on GitHub and appended with the current date's data from the ESRI feature layer.

Our data sources are public and smaller files are in the `data` folder.

1. [Data Sources](#data-sources): [Cases](#covid-19-cases), [Hospital](#hospital-data) and [Testing](#covid-19-testing)    
1. [Helpful Hints for Jupyter Notebooks](#helpful-hints)
1. [Setting up a Conda Environment](#setting-up-a-conda-environment)
1. [Starting with Docker](#starting-with-docker)
1. [Emailing the Report](#emailing-the-report)

## Data Sources
Scripts to ingest, process, and save our data sources are in the [data](./data/) folder. Use the [helpful hints](#helpful-hints) to access the data.

* Johns Hopkins University [global](https://www.arcgis.com/home/item.html?id=c0b356e20b30490c8b8b4c7bb9554e7c) data, [US county](https://www.arcgis.com/home/item.html?id=628578697fb24d8ea4c32fa0c5ae1843) data and [blog post](https://www.esri.com/arcgis-blog/products/product/public-safety/coronavirus-covid-19-data-available-by-county-from-johns-hopkins-university/), and [GitHub repo](https://github.com/CSSEGISandData/COVID-19)
* CA Department of Public Health: [COVID-related open data](https://data.ca.gov/dataset?q=covid&sort=score+desc%2C+metadata_modified+desc)
* `catalog.yml`: data catalog of open data sources and CSVs in the repo

#### COVID-19 Cases
* Global province-level time-series (processed data in S3 bucket)

* US county-level time-series [parquet](https://github.com/CityOfLosAngeles/covid19-indicators/blob/master/data/us-county-time-series.parquet)

* City of LA cases and deaths time-series [CSV](https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators/master/data/city-of-la-cases.csv)

* Scripts: `jhu.py`, `jhu_county.py`, `sync_la_cases.py`. Source: [Google spreadsheet](https://docs.google.com/spreadsheets/d/1Vk7aGL7O0ZVQRySwh6X2aKlbhYlAR_ppSyMdMPqz_aI/edit#gid=1128684515).

#### Hospital Data
* LA County hospital bed and equipment availability (**not used** ) [CSV](https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators/master/data/hospital-availability.csv). Data is available for the [70 largest hospitals](http://file.lacounty.gov/SDSInter/dhs/1070069_HavBedSummary.pdf) in the county and collected in the HavBed survey.

* CA county-level hospitalizations time-series [CSV](https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators/master/data/ca-hospital-and-surge-capacity.csv)

* Scripts: `sync_hospital.py`, `ca_hospital.py`, `ca_ppe.py`. Source: [Google spreadsheet](https://docs.google.com/spreadsheets/d/1rS0Vt-kuxwQKoqZBcaOYOOTc5bL1QZqAqqPSyCaMczQ/edit?usp=sharing).

#### COVID-19 Testing
* LA County COVID-19 tests administered and persons tested [CSV](https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators/master/data/county-city-testing.csv)

* Script: `sync_covid_testing.py`. Source: [Google spreadsheet](https://docs.google.com/spreadsheets/d/1agPpAJ5VNqpY50u9RhcPOu7P54AS0NUZhvA2Elmp2m4/edit?usp=sharing), LA County DPH [RShiny dashboard](http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/)

## Helpful Hints
Jupyter Notebooks can read in both the ESRI feature layer and the CSV. 

Ex: JHU global province-level time-series [feature layer](http://lahub.maps.arcgis.com/home/item.html?id=20271474d3c3404d9c79bed0dbd48580) and [CSV](https://lahub.maps.arcgis.com/home/item.html?id=daeef8efe43941748cb98d7c1f716122)

**Import the CSV**

All you need is the item ID of the CSV item. We use an f-string to construct the URL and use Python `pandas` package to import the CSV.

```
JHU_GLOBAL_ITEM_ID = "daeef8efe43941748cb98d7c1f716122"

JHU_URL = f"http://lahub.maps.arcgis.com/sharing/rest/content/items/{JHU_GLOBAL_ITEM_ID}/data"

TESTING_URL = (
    "https://raw.githubusercontent.com/CityOfLosAngeles/covid19-indicators"
    "master/data/county-city-testing.csv"
)

import pandas as pd

df = pd.read_csv(JHU_URL)
df = pd.read_csv(TESTING_URL)
```

**Import from data catalog**
```
import intake
import pandas as pd

catalog = intake.open_catalog("../catalog.yml")

# See files are inside catalog
list(catalog)

# To open a file called hospital_surge_capacity:
df = catalog.ca_hospital_surge_capacity.read()
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

CORRECT_URL = "https://services5.arcgis.com/7nsPwEMP38bSkCjy/ArcGIS/rest/services/jhu_covid19_time_series/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Province_State%2C+Country_Region%2C+Lat%2C+Long%2C+date%2C+number_of_cases%2C+number_of_deaths%2C+number_of_recovered%2C+ObjectId&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="


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