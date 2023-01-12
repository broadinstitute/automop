#!/usr/bin/env bash
set -e

venv_path=$(pwd)/automop-env
automop_exe=${venv_path}/bin/automop-webui

if [ ! -d $venv_path ]; then
    echo "Intializing virtualenv at ${venv_path}"
    python -m venv automop-env
    pip install -U pip
fi

source ${venv_path}/bin/activate

if [ ! -f $automop_exe ]; then
    echo "Installing automop in virtual env"
    pip install .
fi

$automop_exe $*
