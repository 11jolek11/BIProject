#!/bin/bash
echo "Updating requirements.txt"
source .venv/bin/activate
pip freeze > requirements.txt
deactivate
