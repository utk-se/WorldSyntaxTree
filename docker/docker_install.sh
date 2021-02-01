#!/bin/bash

cd /wst

ls -lah

apt-get update -q
apt-get install -y -q time wait-for-it

python setup.py install
