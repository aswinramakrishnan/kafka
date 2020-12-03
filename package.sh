#!/bin/bash
mkdir temp_docker_zip
python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org -r requirements_lambda.txt -t temp_docker_zip
# See https://github.com/ralienpp/simplipy/blob/master/README.md
rm -f /io/lambda.zip
# zip without any containing folder (or it won't work)
cd temp_docker_zip
zip -r /io/lambda.zip *