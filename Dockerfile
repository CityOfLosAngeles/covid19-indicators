FROM cityofla/ita-data-civis-lab:sha-4888c7e  
RUN curl -sSL https://sdk.cloud.google.com |bash
RUN apt-get update && apt-get install -y \
  golang
WORKDIR /app
COPY ./ ./
#RUN go build for Google Cloud Run web trigger
RUN go build -v -o server
COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
#CMD to start Google Cloud Run server for web trigger
CMD ["/app/server"]
