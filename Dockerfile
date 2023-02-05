#Original base image
#FROM cityofla/ita-data-civis-lab:sha-4888c7e
#new base image from current working image in Prod.
#Allows for no new package install for minor mods of code/data
ARG _DEPLOY_REGION
ARG PROJECT_ID
FROM ${_DEPLOY_REGION}-docker.pkg.dev/${PROJECT_ID}/covid19-indicators/covid19-indicators-base


#cloud sdk pre-installed on ${_DEPLOY_REGION}-docker.pkg.dev/${PROJECT_ID}/covid19-indicators/covid19-indicators-base
#RUN curl -sSL https://sdk.cloud.google.com |bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"


# update release info only
#RUN apt-get update --allow-releaseinfo-change && apt-get install -y \
#  golang
RUN apt-get update --allow-releaseinfo-change

WORKDIR /app
COPY ./ ./
#web hook server already built in image
#RUN go build for Google Cloud Run web trigger
#RUN go build -v -o server

#requirements.txt, conda-requirements.txt are empty files because all apps already installed
COPY conda-requirements.txt /tmp/
RUN conda install --yes -c conda-forge --file /tmp/conda-requirements.txt
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

#Setup already run in image
#RUN python setup.py install

#CMD to start Google Cloud Run server for web trigger
CMD ["/app/server"]
