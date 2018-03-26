#!/usr/bin/env python

import unittest

# If used directly in command line
if __name__ == "__main__":

    test_packages = [
        "tests.common.utils"
    ]

    for test_package in test_packages:
        unittest.main(module=test_package)

