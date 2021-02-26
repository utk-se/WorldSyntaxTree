#!/bin/bash

set -x
set -e

cd /wst

ls -lah

apt-get update -q
apt-get install -y -q time wait-for-it

python -m pip install -r requirements.txt
python setup.py install
