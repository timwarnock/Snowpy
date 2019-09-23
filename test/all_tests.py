#!/usr/bin/env python
# vim: set tabstop=4 shiftwidth=4 autoindent smartindent:
import sys, os
import unittest

# set the path to include parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
# run all tests
loader = unittest.TestLoader()
testSuite = loader.discover(".")
text_runner = unittest.TextTestRunner().run(testSuite)
