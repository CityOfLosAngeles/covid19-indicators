FROM cityofla/ita-data-civis-lab:sha-4888c7e  
RUN apt-get update && apt-get install -y \
  golang
WORKDIR /app
COPY ./ ./
RUN go mod download
RUN go build -mod=readonly -v -o server
COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
CMD ["/app/server"]
