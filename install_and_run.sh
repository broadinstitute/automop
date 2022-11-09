#!/usr/bin/env bash

if [ -d automop ]
then
    while true; do
        read -p "There already exists an automop installation in this directory. Do you want to overwrite it? If not, you can still run automop/run.sh. Overwrite and install new automop installation (y/n)?: " yn
        case $yn in
            [Yy]* ) rm -rf automop; break;;
            [Nn]* ) exit;;
            * ) "Invalid response.";;
        esac
    done
fi
git clone https://github.com/broadinstitute/automop.git
cd automop
rm install_and_run.sh
source run.sh
