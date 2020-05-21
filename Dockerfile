FROM irose/citywide-jupyterlab:8896e05986fc

#COPY conda-requirements.txt /tmp/
#RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

ENTRYPOINT [ "python", "main.py" ]