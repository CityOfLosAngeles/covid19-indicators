From gcr.io/dataflow-templates-base/python3-template-launcher-base:latest as template_launcher
FROM cityofla/ita-data-civis-lab:sha-4888c7e  

RUN curl -sSL https://sdk.cloud.google.com |bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/app/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/dataflow_start.py"
#ENV PIP_NO_DEPS=True

RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
  golang
WORKDIR /app
COPY ./ ./
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
#RUN go build for Google Cloud Run web trigger
RUN go build -v -o server
COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt
COPY *requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/apache_beam_requirements.txt
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache apache-beam[gcp]==2.40.0

COPY --from=apache/beam_python3.7_sdk:2.40.0 /opt/apache/beam /opt/apache/beam
RUN python setup.py install
#CMD to start Google Cloud Run server for web trigger
#CMD ["/app/server"]
# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
