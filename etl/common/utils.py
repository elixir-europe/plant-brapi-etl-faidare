import multiprocessing
import os
import re
import shutil
import signal
import sys
from functools import reduce
from multiprocessing import Pool

from past.types import basestring

default_nb_threads = multiprocessing.cpu_count() + 2


def __get_path(parts, ext, create=False, recreate=False, is_file=True):
    base_path = os.path.join(*parts)
    path = os.path.abspath(base_path + ext)
    create = True if recreate else create
    if recreate and os.path.exists(path):
        if is_file:
            os.remove(path)
        else:
            shutil.rmtree(path)
    if create and not os.path.exists(path):
        if is_file:
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
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = Pool(nb_thread)
    signal.signal(signal.SIGINT, original_sigint_handler)
    terminate = False
    try:
        res = pool.map_async(fn, array_of_args)
        res.get(500)  # Without the timeout this blocking call ignores all signals.
    except KeyboardInterrupt:
        print('Caught KeyboardInterrupt, terminating workers')
        pool.terminate()
        terminate = True
    else:
        pool.close()
    pool.join()
    if terminate:
        sys.exit(1)


# Remove None values from list and dict recursively
def remove_null_and_empty(value, predicate=bool):
    if not value:
        return None
    if isinstance(value, list):
        new_list = list()
        for element in value:
            new_element = remove_null_and_empty(element)
            if predicate(new_element):
                new_list.append(new_element)
        if new_list:
            return new_list
    elif isinstance(value, dict):
        new_dict = dict()
        for key, value in value.items():
            new_value = remove_null_and_empty(value)
            if key and predicate(new_value):
                new_dict[key] = new_value
        if new_dict:
            return new_dict
    else:
        return value


# Replace variables in template strings (ex: "{id}/") by its value in "value_dict" dict (ex: value_dict['id'])
def replace_template(template, value_dict):
    value = template
    if isinstance(template, list):
        value = list()
        for sub_template in template:
            value.append(replace_template(sub_template, value_dict))
    elif isinstance(template, basestring):
        matches = re.findall('({(\w+)})', template, re.DOTALL)
        for var_sub, var_name in matches:
            value = value.replace(var_sub, value_dict[var_name])
    return value


def flatten(value):
    return reduce(lambda acc, x: acc + flatten(x) if isinstance(x, list) else acc + [x], value, [])


def resolve_path(value, path):
    """Recursively list values in dict and list nested objects following a path."""
    if not path:
        return value
    first, rest = path[0], path[1:]
    if isinstance(value, dict) and first in value:
        return remove_null_and_empty(resolve_path(value[first], rest))
    if isinstance(value, list) or isinstance(value, set):
        return remove_null_and_empty(flatten(map(lambda v: resolve_path(v, path), value)))
    return None
