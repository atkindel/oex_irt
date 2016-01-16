## Script to transform ActivityGrade table events to item response data
## Author: Alex Kindel
## Date: 19 October 2015
#
# Changelog
#   [15 Jan 16]: Deprecated, use item_timing.py instead.
#
#

import csv
import os
import sys
import os.path
from os.path import expanduser

# Set infile, outfile, and what data to include
home = expanduser('~')
infile = home + "/VPTL/IRT/Medicine_Sci-Write_Fall2014_ActivityGrade.csv"
outfile = home + "/VPTL/IRT/Medicine_Sci-Write_Fall2014_ActivityGrade_TransformSimple.csv"
simple = True

# Extract all unique learners and unique items
learners_unique = set()
items_unique = set()
data = []

with open(infile, 'rU') as f:
    csvread = csv.DictReader(f)
    for row in csvread:
        if row["'module_type'"] != 'problem':
            continue
        data.append(row)
        learners_unique.add(row["'anon_screen_name'"])
        items_unique.add(row["'module_id'"])

# Ossify sets to lists to preserve ordering
learners = list(learners_unique)
items = list(items_unique)

# How many learners and items did we get?
num_learners = len(learners)
num_items = len(items)
print "Unique learners: %d" % num_learners
print "Unique items: %d" % num_items

# Build and validate item response matrix
item_responses = [['NA' for j in range(num_items)] for i in range(num_learners)]
print len(item_responses) # number of rows (learners) in item response matrix
print len(item_responses[0]) # number of columns for learner at row index 0

# Fill matrix with response data tuples
for row in data:
    learner = row["'anon_screen_name'"]
    item = row["'module_id'"]
    response_Data = None
    if simple:
        if row["'num_attempts'"] == '-1':
            response_data = "-1"
        else:
            response_data = row["'grade'"]
    else:
        response_data = (row["'grade'"], row["'max_grade'"], row["'percent_grade'"], row["'parts_correctness'"], row["'answers'"], row["'num_attempts'"], row["'first_submit'"], row["'last_submit'"])
    ridx = learners.index(learner)
    cidx = items.index(item)
    item_responses[ridx][cidx] = response_data if simple else ";".join(response_data)

# Add header row and fill first column with corresponding learner IDs
header = []
for item in items:
    item = item[-32:] # this makes it easier to reference each problem by ID
    header.append(item)
header.insert(0, "learner_ID")
item_responses.insert(0, header)
for idx, learner in enumerate(learners):
    item_responses[idx+1].insert(0, learner)

# Write to outfile
with open(outfile, 'wb') as w:
    writer = csv.writer(w)
    writer.writerows(item_responses)
