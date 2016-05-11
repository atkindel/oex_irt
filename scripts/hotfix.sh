#!/usr/bin/env bash
# Hotfixes for data issues. To run at end of fetch-raws target.

# Fix problem IDs in courses with CDN containing '.'
sed -e 's/-MedStats_-/-MedStats.-/g' ./data/raws/Medicine_MedStats._Summer2015_ProblemEvents.csv
sed -e 's/-QMSE01_-/-QMSE01.-/g' ./data/raws/Engineering_QMSE01._Autumn2015_ProblemEvents.csv
