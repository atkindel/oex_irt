## EventLog class for OpenEdX tracking log problem events
## Author: Alex Kindel
## Date: 12 November 2015
#
# Changelog
#   [15 Jan 16]: Deprecated, subclass MutableTree from mutable_tree.py instead.
#
#

from collections import MutableSequence

class EventLog(MutableSequence):
    '''
    An EventLog is a sequence that guarantees chronological ordering.

    An EventLog behaves like a list but with three helpful features:
    1. Expects list elements to be tuples of time, event_type, and resource_id.
    2. Initialized with an immutable contextID. Any appends are checked to make
        sure that the resource_id for the event matches the contextID exactly.
    3. Appends are automatically arranged in chronological order.

    This gives us an easier interface for working with event data down the road.
    '''

    # TODO: Test whether this works


    # Initializer. We need an immutable context ID and three empty lists
    # to hold the data from our events.
    def __init__(self, contextID):
        if len(contextID < 1):
            raise ValueError("Require non-empty string for contextID.")
        self._contextID = contextID
        self._resource_list = list()
        self._time_list = list()
        self._type_list = list()

    # These methods work exactly like a normal list, but we'll need to
    # worry about three lists rather than one.
    def __len__(self): return len(self._resource_list)
    def __getitem__(self, idx):
        self.__reorder()
        return self._resource_list[idx], self._time_list[idx], self._type_list[idx]
    def __delitem__(self, idx):
        del self._resource_list[idx]
        del self._time_list[idx]
        del self._type_list[idx]


    # The methods below work a little differently from a normal list.
    # First, we don't want to allow anyone to mess with our time-ordering.
    def insert(self, key, value): raise TypeError("EventLog type does not allow direct item inserts; use append method.")
    def __setitem__(self, key, value): raise TypeError("EventLog type does not allow direct item inserts; use append method.")

    # Similarly, the only time we'll care about index() is for resources, but we
    # want to define a nicer interface, so we'll overrule index() calls.
    def index(self, value): raise TypeError("EventLog type prefers first() and last() calls for value index lookups.")


    # We want to allow appends where we were given a well-defined event and
    # where the given resource_id matches with the contextID we defined before.
    def append(self, value):
        try:
            time, event_type, resource_id = *value
        except ValueError:
            raise ValueError("Event was not in expected time/type/context format.")
        if resource_id != self._contextID:
            raise ValueError("Event resourceID mismatch with log contextID.")

        # For now, we'll just append to the end of our lists
        self._resource_list.append(resource_id)
        self._time_list.append(time)
        self._type_list.append(event_type)

    # We also want to know when the first or the last occurrence of a given
    # resourceID in the event log was.
    def last(self, value):
        self.__reorder()
        return reversed(self._resource_list).index(value)

    def first(self, value):
        self.__reorder()
        return self._resource_list.index(value)

    # To make our lives a little easier, we want to mess with count()
    # so that we can call it on our event_type list or our resource_id list
    # without worrying about which one we're dealing with.
    def count(self, value):
        if value in self._type_list:
            return self._type_list.count(value)
        else:
            return self._resource_list.count(value)


    # The magic behind the scenes: we'll make sure to call __reorder() exactly
    # when the ordering of the list matters.
    def __reorder(self):
        events = zip(self._resource_list, self._time_list, self._type_list)
        events.sort()
        self._resource_list, self._time_list, self._type_list = [list(data) for data in zip(*events)]
