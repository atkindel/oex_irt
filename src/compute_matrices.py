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
ItemAttemptData = namedtuple('ItemAttemptData', ['first_attempt', 'second_attempt', 'third_attempt', 'fourth_attempt', 'fifth_attempt', 'last_attempt', 'n_attempts', 'last_grade', 'first_grade',
                                                 'second_grade', 'third_grade', 'fourth_grade', 'fifth_grade', 'time_spent_attempting'])
ItemTimingData = namedtuple('ItemTimingData', ['first_view', 'time_to_first_attempt', 'time_to_second_attempt', 'time_to_third_attempt', 'time_to_fourth_attempt', 'time_to_fifth_attempt', 'time_to_last_attempt'])


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

        # Indicators for item visibility
        self.ran_in_course = list()

        # Computed data
        self.item_attempts = defaultdict(lambda: defaultdict(ItemAttemptData))
        self.item_timing = defaultdict(lambda: defaultdict(ItemTimingData))

        # Missing or bad data
        self.dropped = list()
        self.negative = list()

        # Unique problem IDs
        self.problemset = set()


    def loadProblemDefs(self):
        '''
        Read problem definitions to determine if problem was visible in course.
        '''
        with open(self.problem_defs, 'rU') as f:
            problems = csv.DictReader(f)
            for item in problems:
                if (item['chapter_idx'] > 0):  # and (~item['staff_only']):
                    self.ran_in_course.append(item['problem_id'])


    @staticmethod
    def extractProblemID(raw_id):
        '''
        Extract problem URI from event. Requires a little dark magic.
        Problem URI will be (e.g.) i4x-Engineering-QMSE-02-problem-0764cd7b11ce41f9982c4d0699fd6bd4_2_1
        '''
        if "://" in raw_id:
            return ""  # Bad check event: no parts data.
        else:
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
        self.loadProblemDefs()
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
                # Also skip if problemID was not visible in the course.
                item_base_id = problemID[-36:-4]
                if len(problemID) < 3 or item_base_id not in self.ran_in_course:
                    self.missing[event.type] += 1  # keep track of what we're losing
                    self.dropped.append(event)
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
        for learner in self.responses:
            for item in self.responses[learner]:
                # Sort events by timestamp
                event_list = sorted(self.events[learner][item], key=lambda e: e.time)
                response_list = sorted(self.responses[learner][item], key=lambda e: e.time)

                # Break out sorted data into corresponding lists
                try:
                    _, _, types, sources, _, pages, rdns, _ = [list(elem) for elem in zip(*event_list)]
                except:
                    pages = ['none']
                    rdns = ['none']  # No browser events found
                try:
                    _, _, _, _, grades, _, _, times = [list(elem) for elem in zip(*response_list)]
                except:
                    continue  # No server events found

                # Catch item metadata
                self.problem_meta[item] = [pages[0], rdns[0]]

                # Store data on item attempts
                calcs = ItemAttemptData(first_attempt=times[0],
                                        second_attempt=times[1] if len(times) > 1 else times[-1],
                                        third_attempt=times[2] if len(times) > 2 else times[-1],
                                        fourth_attempt=times[3] if len(times) > 3 else times[-1],
                                        fifth_attempt=times[4] if len(times) > 4 else times[-1],
                                        last_attempt=times[-1],
                                        n_attempts=len(times),
                                        first_grade=grades[0],
                                        second_grade=grades[1] if len(grades) > 1 else grades[-1],
                                        third_grade=grades[2] if len(grades) > 2 else grades[-1],
                                        fourth_grade=grades[3] if len(grades) > 3 else grades[-1],
                                        fifth_grade=grades[4] if len(grades) > 4 else grades[-1],
                                        last_grade=grades[-1],
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

                item_base_id = item[-32:]
                if item_base_id not in self.ran_in_course:
                    continue  # Skip if item was not actually visible in the course

                # Since we retrieve browse_events pre-sorted, assume first hit for this learner is earliest view
                if item not in seen[learner]:
                    # Apply data to all item URIs that match the one we just found
                    for iuri in filter(lambda i: i.find(item) != -1, self.item_attempts[learner].keys()):
                        calcs = ItemTimingData(first_view=timing,
                                               time_to_first_attempt=self.item_attempts[learner][iuri].first_attempt - timing,
                                               time_to_second_attempt=self.item_attempts[learner][iuri].second_attempt - timing,
                                               time_to_third_attempt=self.item_attempts[learner][iuri].third_attempt - timing,
                                               time_to_fourth_attempt=self.item_attempts[learner][iuri].fourth_attempt - timing,
                                               time_to_fifth_attempt=self.item_attempts[learner][iuri].fifth_attempt - timing,
                                               time_to_last_attempt=self.item_attempts[learner][iuri].last_attempt - timing)
                        if calcs.time_to_first_attempt < 0 or calcs.time_to_last_attempt < 0:
                            self.negative.append({"item": iuri, "learner": learner})  # Log this learner-item pair as timing-negative and continue without adding to final matrix
                            del self.item_attempts[learner][iuri]  # Drop data for this learner-item pair
                        else:
                            self.item_timing[learner][iuri] = calcs
                        seen[learner].append(item)  # Keep track of which items we've calculated first views for

        for learner in self.item_timing.keys():
            if learner not in self.item_attempts.keys():
                self.dropped.append(self.item_attempts[learner])
                del self.item_attempts[learner]  # if we got no timing data, log it and drop this learner

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

            print("Dropped events:", file=out)
            print(json.dumps(self.dropped, indent=4), file=out)

            print("Computed negative timing:", file=out)
            print(json.dumps(self.negative, indent=4), file=out)


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
            for learner in data.keys():
                rowdata = {'learner': learner}
                if not len(data[learner].keys()):
                    continue  # Ignore rows for learners that tried no problems
                for problem in data[learner]:
                    rowdata[problem] = getattr(data[learner][problem], var)
                wrt.writerow(rowdata)


if __name__ == '__main__':

    # Retrieve course IDs
    course_ids = []
    with open('./data/done_courses.txt', 'r') as clist:
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
        timer.loadsummary(outfile='/Users/vpoluser/Code/irt/data/exports/%s/export_summary.txt' % course_id)

        # Write out data to CSV
        for variable in list(set(ItemAttemptData._fields).union(ItemTimingData._fields)):
            timer.writeCSV('%s' % variable, '/Users/vpoluser/Code/irt/data/exports/%s/%s.csv' % (course_id, variable))
        print("Exported data to CSV: %s" % course_id)
