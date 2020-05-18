covid19-indicators
==============================

Key COVID-19 and public health indicators for reopening

Project Organization
------------

    ├── LICENSE
    ├── Makefile                 <- Makefile with commands like `make data` or `make train`
    ├── README.md                <- The top-level README for developers using this project.
    ├── Dockerfile               <- Docker image for this project.
    ├── notebooks                <- Jupyter notebooks.
    ├── references               <- Data dictionaries, manuals, and all other explanatory materials.
    ├── conda-requirements.txt   <- The requirements file for conda installs.
    ├── requirements.txt         <- The requirements file for reproducing the analysis environment,
    │                               e.g generated with `pip freeze > requirements.txt`
    ├── setup.py                 <- makes project pip installable (pip install -e .) 



--------

## Starting with Docker

1. Start with Steps 1-2 above
2. Build Docker container: `docker-compose.exe build`
3. Start Docker container `docker-compose.exe up`
4. Open Jupyter Lab notebook by typing `localhost:8888/lab/` in the browser.

## Setting up a Conda Environment 

1. `conda create --name my_project_name` 

2. `source activate my_project_name
3. `conda install --file conda-requirements.txt -c conda-forge` 
4. `pip install requirements.txt`

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
