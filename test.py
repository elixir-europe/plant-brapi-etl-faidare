#!/usr/bin/env python
import unittest

# If used directly in command line
if __name__ == "__main__":
    testsuite = unittest.TestLoader().discover('.')
    unittest.TextTestRunner(verbosity=1).run(testsuite)

