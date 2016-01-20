## Script to transform EventXtract table events to learner-problem pair timing data
## Author: Alex Kindel
## Date: 12 November 2015
#
# Changelog
#   [19 Jan 16]: Use MutableTree data type to store calculations.
#
#

import csv
import json
import time
import datetime
import random
from os.path import expanduser
from itertools import izip
from mutable_tree import MutableTree
from collections import namedtuple, defaultdict


# Data type for calculations.
class Calculations(MutableTree):
    keylist = ['first_attempt',
        'last_attempt',
        'n_attempts',
        'last_grade',
        'time_to_first_attempt',
        'time_to_last_attempt',
        'time_spent_attempting',
        'first_view'
    ]

# Container class with unrestricted tree semantics
class Tree(MutableTree):
    keylist = None

# JSON encoder subclass for serializing our calculations data type
class MTreeEncoder(json.JSONEncoder):
    def default(self, obj): return obj.data

# JSON encoder subclass for serializing datetime data
class DateEncoder(json.JSONEncoder):
    def default(self, obj): return str(obj)



class ItemTimingComputer(object):
    '''
    Takes data from Stanford-formatted OpenEdX courses and runs computations for
    analysis using IRT models. Depends on table Edx.EdxTrackEvent. Will work if
    fed the entire EdxTrackEvent table for a single course for both args, but will
    run very slowly if so. See event_select.sql for queries to pull optimized
    base tables.
    '''

    def __init__(self, problem_events_dir, browse_events_dir):
        '''
        Constructor for ItemTimingComputer.
        '''
        # Store references to external data that we can run computations over.
        home = expanduser('~')
        self.problem_events = home + problem_events_dir
        self.browse_events = home + browse_events_dir

        # Parsed course event data ###TODO: Use epoch time
        self.events = Tree()
        self.responses = Tree()
        self.registrations = defaultdict(list)
        self.problem_meta = defaultdict(list)

        # Aggregate descriptive statistics
        self.aggregate = defaultdict(int)
        self.missing = defaultdict(int)
        self.ignored = defaultdict(int)

        # Computed data
        self.computations = Calculations()

        # Are we built yet? Have we computed yet?
        self.built = False
        self.computed = False

        # Finally, we want to keep track of all the problems we run into.
        self.problemset = set()

    @staticmethod
    def splitn(obj, char, idx):
        '''
        Convenience method gets the *idx*-th fragment of a string split by *char*.
        '''
        return obj.split(char)[idx]

    @staticmethod
    def extractProblemID(event_type, raw_id):
        '''
        Extract problem ID from event. Requires a little dark magic.
        '''
        problemID = None
        if event_type == 'problem_show':
            problemID = raw_id.split('/')[5]
        elif event_type in ['problem_reset', 'problem_check', 'problem_save']:
            raw_id = raw_id.replace('://', '/').replace('/', '-')
            problemID = raw_id.split('-')[4].split('_')[0]
        else:
            print "Warning: uncaught event_type %s" % event_type
        return problemID

    def build(self):
        '''
        Read data from problem events into computer. Won't run more than once.
        '''
        if self.built:
            print "Problem data structure already built."
        else:
            with open(self.problem_events, 'rU') as f:
                raw_events = csv.DictReader(f)
                RawEvent = namedtuple('raw_event', ['learner', 'item', 'type', 'source', 'grade', 'page', 'rdn', 'time'])
                for row in raw_events:

                    # Read data to namedtuple
                    event = RawEvent(learner=row['anon_screen_name'],
                        item=row['problem_id'],
                        type=row['event_type'],
                        source=row['event_source'],
                        grade=row['success'],
                        page=row['page'],
                        rdn=row['resource_display_name'],
                        time=(datetime.datetime.fromtimestamp(time.mktime(time.strptime(row['time'], '%m/%d/%y %H:%M:%S')))-datetime.datetime(1970,1,1)).total_seconds()
                        )

                    # Skip if we didn't get any problemID information (might be '\N' or empty string)
                    if len(event.item) < 3:
                        self.missing[event.type] += 1 # keep track of what we're losing
                        continue

                    # Drop events that aren't problem_checks
                    if event.type in ['problem_reset', 'problem_save', 'problem_check_fail']:
                        self.ignored[event.type] += 1
                        continue

                    # Otherwise, store aggregate count data
                    self.aggregate[event.type] += 1

                    # Clean up problemID data by extracting identifier and log in problem set
                    problemID = self.extractProblemID(event.type, event.item)
                    self.problemset.add(problemID)

                    # Store event data
                    if event.source == 'browser':
                        self.events[event.learner][problemID] = event
                    else:
                        self.responses[event.learner][problemID] = event

            self.built = True

    @staticmethod
    def paired(iterable):
        '''
        Generator for paired iteration.
        '''
        if not len(iterable) % 2:
            raise ValueError("Iterable has odd number of elements")
        it = iter(iterable)
        while True:
            yield it.next(), it.next()

    def compute(self):
        '''
        Run computations over stored data. Won't run more than once.
        '''
        if not self.built:
            print "Data not built yet."
        elif self.computed:
            print "Data already computed."
        else:
            # First, we'll sort and organize our problem interaction data
            for learner in self.events:
                for item in self.events[learner]:
                    # First, we get our event list
                    event_list = sorted(self.events[learner][item], key=lambda x: x.time)

                    # Next, we can fetch our matching list of server responses
                    response_list = sorted(self.responses[learner][item], key=lambda x: x.time)

                    # Then, we want each column of the event data separately
                    _, _, types, sources, _, pages, rdns, _ = [list(elem) for elem in zip(*event_list)]
                    try:
                        _, _, _, _, grades, _, _, times = [list(elem) for elem in zip(*response_list)]
                    except:
                        grades = ['none']

                    # Store item metadata
                    self.problem_meta[item] = [pages[0], rdns[0]]

                    # Compute and store in our data structures as defined above
                    self.computations[learner][item]['first_attempt'] = times[0]
                    self.computations[learner][item]['last_attempt'] = times[-1]
                    self.computations[learner][item]['time_spent_attempting'] = times[-1] - times[0]
                    self.computations[learner][item]['n_attempts'] = len(times)
                    self.computations[learner][item]['last_grade'] = grades[-1]

            # Now we'll run through our browsing data and do our calculations
            with open(self.browse_events, 'rU') as f:
                events = csv.DictReader(f)
                for row in events:
                    learner = row['anon_screen_name']
                    etype = row['event_type']
                    if 'problem_get' not in etype:
                        continue
                    problem = etype.split('/')[6].split(';')[5].strip('_')
                    timing = (datetime.datetime.fromtimestamp(time.mktime(time.strptime(row['time'], '%m/%d/%y %H:%M:%S')))-datetime.datetime(1970,1,1)).total_seconds()

                    # Replace if our new time is lower for this pair
                    if learner not in self.computations.keys() or problem not in self.computations[learner].keys():
                        continue # We'll skip this row if the learner never tried the problem.
                    if 'first_view' not in self.computations[learner][problem].keys():
                        # This value of timing is our first_view value. Do computations:
                        self.computations[learner][problem]['first_view'] = timing
                        self.computations[learner][problem]['time_to_last_attempt'] = self.computations[learner][problem]['last_attempt'][0] - timing
                        self.computations[learner][problem]['time_to_first_attempt'] = self.computations[learner][problem]['first_attempt'][0] - timing

    def check(self):
        '''
        Check a random learner's data.
        '''
        learner = random.choice(self.computations.keys())
        print learner
        print json.dumps(self.events[learner], indent=4, cls=MTreeEncoder)
        print json.dumps(self.responses[learner], indent=4, cls=MTreeEncoder)
        print json.dumps(self.computations[learner], indent=4, cls=MTreeEncoder)

    def summary(self):
        '''
        Output summary counts.
        '''
        print "Events parsed:"
        print json.dumps(self.aggregate, indent=4)

        print "Events dropped:"
        print json.dumps(self.missing, indent=4)

        print "Events ignored:"
        print json.dumps(self.ignored, indent=4)

    def writeProblemMeta(self, outfile):
        '''
        Given an outfile path, write problem metadata to CSV.
        '''
        with open(outfile, 'w') as out:
            wrt = csv.writer(out)
            wrt.writerow(['problem', 'page', 'resource_display_name'])
            for problem in self.problem_meta:
                rowdata = [problem]
                rowdata.extend(self.problem_meta[problem])
                wrt.writerow(rowdata)

    def writeCSV(self, calc, outfile):
        '''
        Given an outfile path, write the desired calculation to CSV.
        '''
        if calc not in Calculations().keylist:
            raise KeyError("Calculation %s not valid." % calc)
        else:
            with open(outfile, 'w') as out:
                problems = ['learner']
                problems.extend(self.problemset)
                wrt = csv.DictWriter(out, problems, 'NA')
                wrt.writeheader()
                for learner in self.computations:
                    rowdata = {'learner': learner}
                    for problem in self.computations[learner]:
                        rowdata[problem] = self.computations[learner][problem][calc][0]
                    wrt.writerow(rowdata)



if __name__ == '__main__':
    # Set up our event timing computer
    problem_events_dir = "/Code/irt/data/ProblemEvents.csv"
    browse_events_dir = "/Code/irt/data/BrowseEvents.csv"
    timer = ItemTimingComputer(problem_events_dir, browse_events_dir)

    # Build data and compute
    timer.build()
    timer.compute()

    # Check some learner's data
    #timer.check()

    # Output summary counts
    #timer.summary()

    # Write out data to CSV
    for calc in Calculations().keylist:
        timer.writeCSV('%s'%calc, '/Users/vpoluser/Code/irt/data/exports/%s.csv'%calc)
    timer.writeProblemMeta('/Users/vpoluser/Code/irt/data/exports/problem_metadata.csv')
