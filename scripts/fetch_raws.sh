#!/usr/bin/env bash
mkdir -p data/raws
wget -P data/raws/ -r --no-parent -nH --cut-dirs=2 -R "index*" --no-check-certificate https://$DS_EXPORT_U:$DS_EXPORT_P@$DS_HOST/researcher/SU_Kindel_IRT_raws/
