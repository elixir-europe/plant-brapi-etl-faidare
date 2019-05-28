import logging
import math
import multiprocessing
import os
import re
import shutil
import signal
import sys
from functools import partial
from itertools import islice
from multiprocessing import Pool

import collections

import functools
from typing import List

default_nb_threads = multiprocessing.cpu_count() + 2


def __get_path(parts, ext, create=False, recreate=False, is_file=True):
    base_path = os.path.join(*parts)
    # path = os.path.abspath(base_path + ext)
    path = base_path + ext
    create = recreate or create
    if recreate and os.path.exists(path):
        if is_file:
            os.remove(path)
        else:
            shutil.rmtree(path)
    if create and not os.path.exists(path):
        if is_file:
            dir_name = os.path.dirname(path)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            open(path, 'a').close()
        else:
            os.makedirs(path)
    return path


def get_folder_path(parts, create=False, recreate=False):
    return __get_path(parts, '', create, recreate, False)


def get_file_path(parts, ext='', create=False, recreate=False):
    return __get_path(parts, ext, create, recreate, True)


# Join url path without duplicate '/'
def join_url_path(*parts):
    return '/'.join(s.strip('/') for s in parts)


# Run a function on a thread pool with an array of args for each function call
# Handles interrupts correctly
def pool_worker(fn, array_of_args, nb_thread=default_nb_threads):
    pool = Pool(nb_thread)
    try:
        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, original_sigint_handler)
    except:
        # Listening for signal won't work on non main thread
        pass
    terminate = False
    res = None
    try:
        workers = pool.map_async(fn, array_of_args)
        res = workers.get(5000)  # Without the timeout this blocking call ignores all signals.
    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt, terminating workers')
        pool.terminate()
        terminate = True
    else:
        pool.close()
    pool.join()
    if terminate:
        sys.exit(1)
    return res


def is_collection(value):
    return not isinstance(value, str) and isinstance(value, collections.abc.Iterable)


def remove_falsey(value, predicate=bool):
    """Remove falsey values for collections and both keys and values for dictionaries"""
    if not predicate(value):
        return None
    if is_collection(value):
        original_type = type(value)

        if isinstance(value, dict):
            def filter_process(entry):
                new_entry = remove_falsey(entry, predicate)
                try:
                    key, value = new_entry
                    if predicate(value):
                        return [key, value]
                except ValueError:
                    return None
            value = value.items()
        else:
            filter_process = partial(remove_falsey, predicate=predicate)
        filter_none = partial(filter, lambda x: x is not None)
        filtered_values = filter_none(map(filter_process, value))
        result = as_collection_type(original_type, filtered_values)
        if predicate(result):
            return result
        return None
    return value


def remove_none(value):
    """Remove `None` values (and keys) from collections and dictionaries"""
    return remove_falsey(value, predicate=lambda x: x is not None)


def remove_empty(value):
    """Remove `None` values and empty collections and strings"""
    return remove_falsey(
        value, predicate=lambda x: x is not None and ((not isinstance(x, collections.abc.Sized)) or len(x) > 0))


# Replace variables in template strings (ex: "{id}/") by its value in "value_dict" dict (ex: value_dict['id'])
def replace_template(template, value_dict):
    value = template
    if isinstance(template, list):
        value = list()
        for sub_template in template:
            value.append(replace_template(sub_template, value_dict))
    elif isinstance(template, str):
        matches = re.findall('({(\w+)})', template, re.DOTALL)
        for var_sub, var_name in matches:
            value = value.replace(var_sub, value_dict[var_name])
    return value


def is_list_like(element):
    return is_collection(element) and not isinstance(element, dict)


def flatten_it(iterable):
    for element in iterable:
        if is_list_like(element):
            for sub_element in flatten_it(element):
                yield sub_element
        else:
            yield element


def flatten(value):
    return list(flatten_it(value))


def distinct(values):
    seen = set()
    if values:
        for value in values:
            if value not in seen:
                seen.add(value)
                yield value


def get_in(values, path):
    """
    Get in nested values (dict/list) through a path
    """
    if not path or not values:
        return values
    if isinstance(values, dict):
        first, rest = path[0], path[1:]
        if first in values:
            return remove_none(get_in(values[first], rest)) or None
        else:
            return None
    if is_list_like(values):
        return remove_none(flatten(map(lambda value: get_in(value, path), values))) or None
    return None


def as_collection_type(type, value):
    if type in [dict, list, set, tuple]:
        if not value:
            return type()
        return type(value)
    return value


def as_list(x):
    if not x:
        return []
    if isinstance(x, list):
        return x
    if is_list_like(x):
        return list(x)
    return [x]


def create_logger(name, log_file, verbose):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(file_handler)

    # Log stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter(f'[{name}] %(message)s'))
    level = logging.DEBUG if verbose else logging.INFO
    stdout_handler.setLevel(level)
    logger.addHandler(stdout_handler)
    return logger


def split_every(n, iterable):
    accumulator = list()
    for element in iter(iterable):
        accumulator.append(element)
        if len(accumulator) >= n:
            yield accumulator
            accumulator = list()
    if accumulator:
        yield accumulator


def split_parts(iterable, parts=2):
    return split_every(math.ceil(len(iterable)/parts), iterable)


def cat_it(*iterables):
    for iterable in iterables:
        for element in iterable:
            yield element


def first(it):
    return next(iter(it))


def compose(*functions):
    return functools.reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def update_in(dictionary, path, value):
    """
    Update a dict value at given path

    update_in({}, ['a', 'b', 'c'], 'd')
    => {'a':{'b':{'c':'d'}}}
    """
    if path:
        current = dictionary
        for pathPart in path[:-1]:
            previous = current
            try:
                current = current[pathPart]
            except KeyError:
                previous[pathPart] = current = {}
        current[path[-1]] = value
    return dictionary
