#!/bin/bash
docker pull us.gcr.io/ita-datalake/covid19-indicators/tf-covid19-indicators-stage-1:e674ab0ac4c2671da86fcb1aa641e3ec5392f375
docker tag 1849afd0ad4f us-west2-docker.pkg.dev/ita-datalake/covid19-indicators/covid19-indicators-base
docker tag 1849afd0ad4f us-west2-docker.pkg.dev/ita-datalakepoc/covid19-indicators/covid19-indicators-base
gcloud auth login
gcloud config set project ita-datalake
gcloud auth configure-docker us-west2-docker.pkg.dev
docker push us-west2-docker.pkg.dev/ita-datalake/covid19-indicators/covid19-indicators-base
docker push us-west2-docker.pkg.dev/ita-datalakepoc/covid19-indicators/covid19-indicators-base
