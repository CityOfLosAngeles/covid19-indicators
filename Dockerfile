FROM irose/citywide-civis-lab:55c470372865

COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
