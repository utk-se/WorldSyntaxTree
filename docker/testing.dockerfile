FROM python:3.9

COPY . /wst/

WORKDIR /wst/

RUN bash docker/docker_install.sh
