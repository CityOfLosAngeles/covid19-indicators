FROM irose/citywide-civis-lab:sha-84523d5

COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt