import multiprocessing
import os
import re
import shutil
import signal
import sys
from functools import reduce
from multiprocessing import Pool

import logging
default_nb_threads = multiprocessing.cpu_count() + 2


def __get_path(parts, ext, create=False, recreate=False, is_file=True):
    base_path = os.path.join(*parts)
    # path = os.path.abspath(base_path + ext)
    path = base_path + ext
    create = True if recreate else create
    if recreate and os.path.exists(path):
        if is_file:
            os.remove(path)
        else:
            shutil.rmtree(path)
    if create and not os.path.exists(path):
        if is_file:
            dir = os.path.dirname(path)
            if not os.path.exists(dir):
                os.makedirs(dir)
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


# Remove None values from list and dict recursively
def remove_null_and_empty(value, predicate=bool):
    if not value:
        return None
    is_list = isinstance(value, list)
    is_set = isinstance(value, set)
    is_dict = isinstance(value, dict)
    if is_dict or is_list or is_set:
        new_value = (type(value))()
        for element in value:
            key = True
            if is_dict:
                key = element
                element = value[element]
            new_element = remove_null_and_empty(element)
            if key and predicate(new_element):
                if is_dict:
                    new_value[key] = new_element
                if is_list:
                    new_value.append(new_element)
                if is_set:
                    new_value.add(new_element)
        if new_value:
            return new_value
    else:
        return value


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


def create_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(file_handler)

    # Log stdout
    # stdout_handler = logging.StreamHandler(sys.stdout)
    # stdout_handler.setLevel(logging.INFO)
    # logger.addHandler(stdout_handler)

    return logger
