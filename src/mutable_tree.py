## Class: MutableTree
## Class implements an autovivifying tree with abstract leaf nodes. User must subclass
##  and assign a value to the class attribute 'keylist'. Class also defines its own
##  JSONEncoder subclass for easier printing. Assigning `None` to keylist results in
##  an unrestricted autovivifying tree.
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
        if self.__class__.keylist is None or key in self.__class__.keylist:
            if self.data[key] is not None or type(self.data[key]) is list:
                existing = self.data[key]
                self.data[key] = list()
                self.data[key].extend(existing)
                self.data[key].append(value)
            else:
                self.data[key] = value
        else:
            raise KeyError("Key '%s' not in key list of class '%s'" % (key, self.__class__.__name__))

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)



## Unit tests ##
if __name__ == '__main__':


    ## First, we subclass MutableTree with a keylist and instantiate it
    class ExampleMutableTree(MutableTree):
        keylist = ['d', 'e', 'f']

        class MutableTreeEncoder(json.JSONEncoder):
            def default(self, obj): return obj.data

    test = ExampleMutableTree()

    ## Test 1: leaf key in keylist
    print "Test 1 result: "
    try:
        test['a']['b']['c']['d'] = 'e'
        print json.dumps(test, indent=4, cls=ExampleMutableTree.MutableTreeEncoder)
        print "**Test 1 passed.\n"
    except Exception as e:
        print repr(e)
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


    ## Ensure that keylist = `None` results in an unrestricted tree.
    class ExampleFreeTree(MutableTree):
        keylist = None

        class MutableTreeEncoder(json.JSONEncoder):
            def default(self, obj): return obj.data

    test2 = ExampleFreeTree()

    print "Test 3 result: "
    try:
        test2['a']['b']['c'] = 'd'
        print json.dumps(test2, indent=4, cls=ExampleFreeTree.MutableTreeEncoder)
        print "**Test 3 passed.\n"
    except Exception as e:
        print repr(e)
        print "**Test 3 failed.\n"


    ## Ensure a tree can accept leaf nodes of arbitrary type.
    class ExampleListTree(MutableTree):
        keylist = None

        class MutableTreeEncoder(json.JSONEncoder):
            def default(self, obj): return obj.data

    test3 = ExampleListTree()

    print "Test 4 result: "
    try:
        test3['a']['b']['c'] = list()
        test3['a']['b']['c'].append('d')
        print json.dumps(test3, indent=4, cls=ExampleListTree.MutableTreeEncoder)
        print "**Test 4 passed.\n"
    except Exception as e:
        print repr(e)
        print "**Test 4 failed.\n"
