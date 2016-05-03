#!/usr/bin/env python
# Script to transform EventXtract table events to learner-problem pair timing data
# Author: Alex Kindel
# Date: 2 May 2016
#

from __future__ import print_function
import csv
import json
import time
import datetime
import random
import math
import os
import shutil
from os.path import expanduser
from itertools import izip
from collections import namedtuple, defaultdict


Event = namedtuple('Event', ['learner', 'item', 'type', 'source', 'grade', 'page', 'rdn', 'time'])
ItemAttemptData = namedtuple('ItemAttemptData', ['first_attempt', 'last_attempt', 'n_attempts', 'last_grade', 'first_grade', 'time_spent_attempting'])
ItemTimingData = namedtuple('ItemTimingData', ['first_view', 'time_to_first_attempt', 'time_to_last_attempt'])


class ItemMatrixComputer(object):
    '''
    Takes data from Stanford-formatted OpenEdX courses and runs computations for
    analysis using IRT models. Depends on table Edx.EdxTrackEvent. Will work if
    fed the entire EdxTrackEvent table for a single course for both args, but will
    run *very* slowly if so. See directory sql/ for queries to pull base tables.
    '''

    def __init__(self, problem_events_dir, browse_events_dir, problem_defs_dir, registrations_dir):
        '''
        Constructor for ItemMatrixComputer.
        '''
        # Paths to external data
        home = expanduser('~')
        self.problem_events = home + problem_events_dir
        self.browse_events = home + browse_events_dir
        self.problem_defs = home + problem_defs_dir
        self.reg_events = home + registrations_dir

        # Parsed course event data
        self.events = defaultdict(lambda: defaultdict(list))
        self.responses = defaultdict(lambda: defaultdict(list))
        self.problem_meta = defaultdict(tuple)

        # Aggregate descriptive statistics
        self.aggregate = defaultdict(int)
        self.missing = defaultdict(int)
        self.ignored = defaultdict(int)

        # Computed data
        self.item_attempts = defaultdict(lambda: defaultdict(ItemAttemptData))
        self.item_timing = defaultdict(lambda: defaultdict(ItemTimingData))

        # Unique problem IDs
        self.problemset = set()


    @staticmethod
    def extractProblemID(raw_id):
        '''
        Extract problem URI from event. Requires a little dark magic.
        Problem URI will be (e.g.) i4x-Engineering-QMSE-02-problem-0764cd7b11ce41f9982c4d0699fd6bd4_2_1
        '''
        raw_id = raw_id.replace('://', '/').replace('/', '-')
        problemID = raw_id.replace("input_", "").replace("""%5B%5D""", "")
        return problemID


    @staticmethod
    def convertTime(timestamp):
        '''
        Given an event timestamp, return Unix epoch time.
        '''
        return (datetime.datetime.fromtimestamp(time.mktime(time.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f'))) - datetime.datetime(1970, 1, 1)).total_seconds()


    def read_attempts(self):
        '''
        Read data from problem events into computer.
        '''
        with open(self.problem_events, 'rU') as p:
            prob_events = csv.DictReader(p)
            for row in prob_events:

                # Parse problem URI
                problemID = self.extractProblemID(row['problem_id'])

                # Read data to namedtuple
                event = Event(learner=row['anon_screen_name'],
                              item=problemID,
                              type=row['event_type'],
                              source=row['event_source'],
                              grade=row['success'],
                              page=row['page'],
                              rdn=row['resource_display_name'],
                              time=self.convertTime(row['time']))

                # Skip if we didn't get any problemID information (might be '\N' or empty string)
                if len(event.item) < 3:
                    self.missing[event.type] += 1  # keep track of what we're losing
                    continue

                # Drop events that aren't valid problem submissions
                if event.type in ['problem_reset', 'problem_save', 'problem_check_fail']:
                    self.ignored[event.type] += 1
                    continue

                # Otherwise, store aggregate count data
                self.aggregate[event.type] += 1

                # Log problem ID in problem set
                self.problemset.add(problemID)

                # Store event data
                if event.source == 'browser':
                    self.events[event.learner][problemID].append(event)
                else:
                    self.responses[event.learner][problemID].append(event)


    def compute_attempts(self):
        '''
        Run computations over stored data on item attempts.
        '''
        for learner in self.events:
            for item in self.events[learner]:
                # Sort events by timestamp
                event_list = sorted(self.events[learner][item], key=lambda e: e.time)
                response_list = sorted(self.responses[learner][item], key=lambda e: e.time)

                # Break out sorted data into corresponding lists
                _, _, types, sources, _, pages, rdns, _ = [list(elem) for elem in zip(*event_list)]
                try:
                    _, _, _, _, grades, _, _, times = [list(elem) for elem in zip(*response_list)]
                except:
                    grades = ['none']

                # Catch item metadata
                self.problem_meta[item] = [pages[0], rdns[0]]

                # Store data on item attempts
                calcs = ItemAttemptData(first_attempt=times[0],
                                        last_attempt=times[-1],
                                        n_attempts=len(times),
                                        last_grade=grades[-1],
                                        first_grade=grades[0],
                                        time_spent_attempting=times[-1] - times[0])
                self.item_attempts[learner][item] = calcs


    def compute_timing(self):
        '''
        Run computations over stored data on item attempts.
        '''
        with open(self.browse_events, 'rU') as f:
            events = sorted(csv.DictReader(f), key=lambda e: e['time'])
            seen = defaultdict(list)
            for row in events:
                # Retrieve data from row
                learner = row['anon_screen_name']
                etype = row['event_type']
                item = etype.split('/')[6].replace(";_", "-").replace(":--", "-")
                timing = self.convertTime(row['time'])

                # Since we retrieve browse_events pre-sorted, assume first hit for this learner is earliest view
                if item not in seen[learner]:
                    # Apply data to all item URIs that match the one we just found
                    for iuri in filter(lambda i: i.find(item) != -1, self.item_attempts[learner].keys()):
                        calcs = ItemTimingData(first_view=timing,
                                               time_to_first_attempt=self.item_attempts[learner][iuri].first_attempt - timing,
                                               time_to_last_attempt=self.item_attempts[learner][iuri].last_attempt - timing)
                        self.item_timing[learner][iuri] = calcs
                        seen[learner].append(item)  # Keep track of which items we've calculated first views for


    def loadcheck(self, outfile):
        '''
        Check a random learner's data.
        '''
        with open(outfile, 'a') as out:
            learner = random.choice(self.events.keys())
            print(learner, file=out)
            print(json.dumps(self.events[learner], indent=4), file=out)
            print(json.dumps(self.responses[learner], indent=4), file=out)
            print(json.dumps(self.item_attempts[learner], indent=4), file=out)
            print(json.dumps(self.item_timing[learner], indent=4), file=out)


    def loadsummary(self, outfile):
        '''
        Output summary counts.
        '''
        with open(outfile, 'a') as out:
            print("Events parsed:", file=out)
            print(json.dumps(self.aggregate, indent=4), file=out)

            print("Events dropped:", file=out)
            print(json.dumps(self.missing, indent=4), file=out)

            print("Events ignored:", file=out)
            print(json.dumps(self.ignored, indent=4), file=out)


    def writeCSV(self, var, outfile):
        '''
        Given an outfile path, write the desired calculation to CSV.
        '''
        with open(outfile, 'w') as out:
            problems = ['learner']
            problems.extend(self.problemset)
            wrt = csv.DictWriter(out, problems, 'NA')
            wrt.writeheader()
            data = self.item_attempts if var in ItemAttemptData._fields else self.item_timing
            for learner in data:
                rowdata = {'learner': learner}
                for problem in data[learner]:
                    rowdata[problem] = getattr(data[learner][problem], var)
                wrt.writerow(rowdata)


if __name__ == '__main__':

    # Retrieve course IDs
    course_ids = []
    with open('./data/courselist.txt', 'r') as clist:
        for course in clist:
            course_ids.append(course.replace('/', '_').rstrip())

    for course_id in course_ids:

        # Ensure output directory exists
        export_dir = expanduser("~") + "/Code/irt/data/exports/%s/" % course_id
        try:
            os.mkdir(export_dir, 0775)
        except OSError:
            shutil.rmtree(export_dir)
            os.mkdir(export_dir, 0775)

        # Set up event timing computer
        problem_events_dir = "/Code/irt/data/raws/%s_ProblemEvents.csv" % course_id
        browse_events_dir = "/Code/irt/data/raws/%s_BrowseEvents.csv" % course_id
        problem_defs_dir = "/Code/irt/data/raws/%s_ProblemMetadata.csv" % course_id
        registrations_dir = "/Code/irt/data/raws/%s_Registrations.csv" % course_id
        timer = ItemMatrixComputer(problem_events_dir, browse_events_dir, problem_defs_dir, registrations_dir)

        # Parse data
        timer.read_attempts()
        print("Loaded item attempt data: %s" % course_id)
        timer.compute_attempts()
        print("Computed item attempt data: %s" % course_id)
        timer.compute_timing()
        print("Computed item timing data: %s" % course_id)

        # Check that load worked
        timer.loadcheck(outfile='/Users/vpoluser/Code/irt/data/exports/%s/export_summary.txt' % course_id)
        timer.loadsummary(outfile='/Users/vpoluser/Code/irt/data/exports/%s/export_summary.txt' % course_id)

        # Write out data to CSV
        for variable in list(set(ItemAttemptData._fields).union(ItemTimingData._fields)):
            timer.writeCSV('%s' % variable, '/Users/vpoluser/Code/irt/data/exports/%s/%s.csv' % (course_id, variable))
        print("Exported data to CSV: %s" % course_id)
