FROM python:3.8

COPY . /wst/

WORKDIR /wst/

RUN bash docker/docker_install.sh
