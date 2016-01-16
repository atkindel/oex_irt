## Class: MutableTree
## Class implements an autovivifying tree with abstract leaf nodes. User must subclass
##  and assign a value to the class attribute 'keylist'. Class also defines its own
##  JSONEncoder subclass for easier printing.
##
## Author: Alex Kindel
## Date: 15 January 2016

from collections import defaultdict, MutableMapping
import json

class MutableTree(MutableMapping):

    def __init__(self):
        self.data = defaultdict(self.__class__)

    @property
    def keylist(self):
        raise NotImplementedError

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        if key in self.__class__.keylist:
            self.data[key] = value
        else:
            raise KeyError("Key '%s' not in key list of class '%s'" % (key, self.__class__.__name__))

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    class MutableTreeEncoder(json.JSONEncoder):
        def default(self, obj): return obj.data


## Unit tests ##
if __name__ == '__main__':

    ## First, we subclass MutableTree with a keylist and instantiate it
    class ExampleMutableTree(MutableTree):
        keylist = ['d', 'e', 'f']
    test = ExampleMutableTree()

    ## Test 1: leaf key in keylist
    print "Test 1 result: "
    try:
        test['a']['b']['c']['d'] = 'e'
        print json.dumps(test, indent=4, cls=ExampleMutableTree.MutableTreeEncoder)
        print "**Test 1 passed.\n"
    except:
        print "**Test 1 failed.\n"

    ## Test 2: leaf key NOT in keylist
    print "Test 2 result: "
    try:
        test['a']['b'] = 'c'
        print json.dumps(test, indent=4, cls=ExampleMutableTree.MutableTreeEncoder)
        print "**Test 2 failed.\n"
    except KeyError as ke:
        print repr(ke)
        print "**Test 2 passed.\n"
