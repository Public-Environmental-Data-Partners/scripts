#!/bin/bash

baseDir="download"

for stateDir in "$baseDir"/*/; do
    state=$(basename "$stateDir")
    zip -j "./facility_detail_xml/${state}.zip" "$stateDir"/*
done
