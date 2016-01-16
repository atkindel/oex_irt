## Script to transform EventXtract table events to learner-problem pair timing data
## Author: Alex Kindel
## Date: 12 November 2015

import csv
import json
import time
import datetime
import random
from os.path import expanduser
from itertools import izip
from collections import namedtuple, defaultdict, MutableMapping

# Data type for calculations. Behaves like a dictionary, but with an immutable key set.
class Calculations(MutableMapping):
    '''
    Data type defines a dictionary with a set of allowed keys.
    '''
    # Class variable that circumscribes allowed calculation keys
    CALCS = ['first_attempt',
             'last_attempt',
             'last_grade',
             'time_to_first_attempt',
             'time_to_last_attempt',
             'time_spent_attempting',
             'n_attempts',
             'first_view'
            ]

    def __init__(self):
        self.data = dict.fromkeys(Calculations.CALCS)

    # Decorator to check key on setter method
    def restrict_key(func):
        def check(self, key, value):
            if key in Calculations.CALCS:
                return func(self, key, value)
            else:
                raise KeyError("Key '%s' not in list of permitted keys." % key)
        return check

    def __getitem__(self, key):
        return self.data[key]

    @restrict_key
    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)


# JSON encoder subclass handles our Calculations data type
class CalcEncoder(json.JSONEncoder):
    def default(self, o):
        return o.data

class DateEncoder(json.JSONEncoder):
    def default(self, o):
        return str(o)


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

        # Also store a couple of data structures to store data and computations.
        self.events = self.tree(list)
        self.responses = self.tree(list)
        self.computations = self.tree(Calculations)
        self.registrations = defaultdict(list)
        self.problem_meta = defaultdict(list)
        self.aggregate = self.stats()
        self.missing = self.stats()
        self.ignored = self.stats()

        # Are we built yet? Have we computed yet?
        self.built = False
        self.computed = False

        # Finally, we want to keep track of all the problems we run into.
        self.problemset = set()

    # TODO: This could have a better interface if it were its own data type
    # Also try doing def tree(): return defaultdict(tree)-- this lets you be less explicit about leaf type
    @staticmethod
    def tree(ddtype):
        '''
        Use defaultdicts to simulate a tree of depth 2 with leaves of type 'ddtype'
        '''
        return defaultdict(lambda: defaultdict(ddtype))

    @staticmethod
    def splitn(obj, char, idx):
        '''
        Convenience method gets the *idx*-th fragment of a string split by *char*.
        '''
        return obj.split(char)[idx]

    @staticmethod
    def stats():
        '''
        We want to accumulate a dictionary of counts for each of these events
        '''
        return {'problem_check': 0, 'problem_show': 0, 'problem_reset': 0, 'problem_save': 0, 'problem_check_fail': 0}

    @staticmethod
    def extractProblemID(event_type, raw_id):
        '''
        Extract problem ID from event. Requires a little dark magic.
        '''
        problemID = None
        if event_type == 'problem_show':
            problemID = raw_id.splitn('/', 5)
        elif event_type in ['problem_reset', 'problem_check', 'problem_save']:
            raw_id = raw_id.replace('://', '/').replace('/', '-')
            problemID = raw_id.splitn('-', 4).splitn('_', 0)
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
                for row in raw_events:
                    # Get data
                    learnerID = row['anon_screen_name']
                    raw_problemID = row['problem_id']
                    event_type = row['event_type']
                    source = row['event_source']
                    grade = row['success']
                    page = row['page']
                    rdn = row['resource_display_name']
                    timing = datetime.datetime.fromtimestamp(time.mktime(time.strptime(row['time'], '%m/%d/%y %H:%M:%S')))
                    #timing = time.strptime(row['time'], '%m/%d/%y %H:%M:%S')

                    # Skip if we didn't get any problemID information (might be '\N' or empty string)
                    if len(raw_problemID) < 3:
                        self.missing[event_type] += 1 # keep track of what we're losing
                        continue

                    # Drop events that aren't problem_checks
                    if event_type in ['problem_reset', 'problem_save', 'problem_check_fail']:
                        self.ignored[event_type] += 1
                        continue

                    # Clean up problemID data by extracting identifier; log in problem set
                    problemID = self.extractProblemID(event_type, raw_problemID)
                    self.problemset.add(problemID)

                    # Store aggregate count data
                    self.aggregate[event_type] += 1

                    # Store specific event data
                    parsed_event = (timing, event_type, page, rdn, source, grade)
                    if source == 'browser':
                        self.events[learnerID][problemID].append(parsed_event)
                    else:
                        self.responses[learnerID][problemID].append(parsed_event)

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
                for problem in self.events[learner]:
                    # First, we get our event list
                    event_list = sorted(self.events[learner][problem])

                    # Next, we can fetch our matching list of server responses
                    response_list = sorted(self.responses[learner][problem])

                    # Then, we want each column of the event data separately
                    times, types, pages, rdns, sources, grades = [list(elem) for elem in zip(*event_list)]

                    # Store problem metadata
                    self.problem_meta[problem] = [pages[0], rdns[0]]

                    # Compute and store in our data structures as defined above
                    # Leave these calculable for the time being
                    data = Calculations()
                    data['first_attempt'] = times[0]
                    data['last_attempt'] = times[-1]
                    data['n_attempts'] = len(times)
                    data['last_grade'] = grades[-1]
                    self.computations[learner][problem] = data

            # Now we'll run through our browsing data and do our calculations
            with open(self.browse_events, 'rU') as f:
                events = csv.DictReader(f)
                for row in events:
                    learner = row['anon_screen_name']
                    etype_str = row['event_type']
                    if 'problem_get' not in etype_str:
                        continue
                    problem = etype_str.splitn('/', 6).split(';', 5).strip('_')
                    timing = datetime.datetime.fromtimestamp(time.mktime(time.strptime(row['time'], '%m/%d/%y %H:%M:%S')))

                    # Replace if our new time is lower for this pair
                    if learner not in self.computations or problem not in self.computations[learner]:
                        continue # We'll skip this row if the learner never tried the problem.
                    vals = self.computations[learner][problem]
                    if vals['first_view'] == None:
                        # This value of timing is our first_view value. Do computations:
                        first_try = vals['first_attempt']
                        last_try = vals['last_attempt']
                        self.computations[learner][problem]['time_to_last_attempt'] = (last_try - timing).seconds
                        self.computations[learner][problem]['time_to_first_attempt'] = (first_try - timing).seconds
                        self.computations[learner][problem]['time_spent_attempting'] = (last_try - first_try).seconds

                        # We'll serialize our timestamps before we put them back
                        self.computations[learner][problem]['first_attempt'] = str(first_try)
                        self.computations[learner][problem]['last_attempt'] = str(last_try)
                        self.computations[learner][problem]['first_view'] = str(timing)

    def check(self):
        '''
        Check a random learner's data.
        '''
        learner = random.choice(self.computations.keys())
        print learner
        print json.dumps(self.events[learner], indent=4, cls=DateEncoder)
        print json.dumps(self.responses[learner], indent=4, cls=DateEncoder)
        print json.dumps(self.computations[learner], indent=4, cls=CalcEncoder)

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
        if calc not in Calculations().CALCS:
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
                        rowdata[problem] = self.computations[learner][problem][calc]
                    wrt.writerow(rowdata)



if __name__ == '__main__':
    # Set up our event timing computer
    problem_events_dir = "/Code/irt/data/ProblemEvents.csv" #TODO: this should track server problem_check responses too
    browse_events_dir = "/Code/irt/data/BrowseEvents.csv"
    timer = ItemTimingComputer(problem_events_dir, browse_events_dir)

    # Build data and compute
    timer.build()
    timer.compute()

    # Check some learner's data
    timer.check()

    # Output summary counts
    timer.summary()

    # Write out some data to CSV
    # timer.writeCSV('time_to_last_attempt', '/Users/vpoluser/VPTL/IRT/time_to_last_attempt.csv')
    # timer.writeProblemMeta('/Users/vpoluser/VPTL/IRT/problem_metadata.csv')
