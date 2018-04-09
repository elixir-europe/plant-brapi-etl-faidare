import itertools

import collections
from functools import reduce, partial

import lark
import re

from etl.common.utils import remove_null, resolve_path, as_list, flatten, remove_falsey

parser = lark.Lark('''
FIELD: WORD 
field_path: ("." FIELD?)+

object_path: field_path WS* "=>" WS*

value_path: field_path (WS* "+" WS* field_path)* 

start: "{" WS* object_path* WS* value_path WS* "}"

%import common.WS
%import common.WORD
''')


def get_path(field_path):
    if not field_path or not field_path.children:
        return []
    return remove_null(list(map(lambda field: field.value, field_path.children)))


def remove_white_space_token(tree):
    if isinstance(tree, lark.tree.Tree):
        def is_ws(child):
            if not (isinstance(child, lark.lexer.Token) and child.type == "WS"):
                return child
        without_ws = filter(is_ws, tree.children)
        new_children = remove_null(list(map(remove_white_space_token, without_ws)))
        return lark.tree.Tree(tree.data, new_children)
    return tree


def coll_as_str(value):
    if isinstance(value, str):
        return value
    if isinstance(value, collections.Iterable):
        return ''.join(map(coll_as_str, value))
    return str(value)


def resolve_value_template(tree, data, data_index):
    for child in tree.children:
        if child.data == "object_path":
            path = get_path(child.children[0])
            ids = resolve_path(data, path)
            if not ids:
                return None
            data = remove_null(list(map(lambda id: data_index.get(id), ids)))
        elif child.data == "value_path":
            if len(child.children) == 1:
                path = get_path(child.children[0])
                return resolve_path(data, path)
            else:
                new_value = []
                for data in as_list(data):
                    field_values = remove_falsey(map(
                        lambda field_path: as_list(resolve_path(data, get_path(field_path))) or None,
                        child.children
                    ))
                    product = itertools.product(*field_values)
                    joined = map(
                        lambda field_value: reduce(
                            lambda acc, s: s if s.startswith(acc) else acc + " " + s,
                            field_value,
                            ""
                        ),
                        product
                    )
                    if joined:
                        new_value.extend(joined)
                return list(collections.OrderedDict.fromkeys(remove_falsey(new_value)))


def resolve_string_template(template_string, data, data_index):
    if re.match(r"^[^{}]+$", template_string):
        return template_string
    if re.match(r"^{[^{}]+}$", template_string):
        try:
            raw_tree = parser.parse(template_string)
            tree = remove_white_space_token(raw_tree)
        except lark.lexer.UnexpectedInput as e:
            raise Exception("Could not parse template '{}'".format(template_string), e)
        return resolve_value_template(tree, data, data_index)
    else:
        tokens = re.findall(r"({[^}]*}|[^{}]+)", template_string)
        return resolve_join_template({'{join}': tokens}, data, data_index)


def resolve_if_template(template, data, data_index):
    if_test, then_branch, else_branch = [template.get(k) for k in ('{if}', '{then}', '{else}')]

    if not if_test:
        raise Exception("Empty test in '{{if}}' template '{}'".format(template))
    if not then_branch:
        raise Exception("Missing '{{then}}' branch in '{{if}}' template '{}'".format(template))

    if_result = resolve(if_test, data, data_index)
    if if_result:
        return resolve(then_branch, data, data_index)
    elif else_branch:
        return resolve(else_branch, data, data_index)


def resolve_join_template(template, data, data_index):
    join_elements = template.get('{join}')

    if not join_elements:
        raise Exception("Empty '{{join}}' template '{}'".format(join_elements))
    if not isinstance(join_elements, list):
        raise Exception("'{{join}}' template is not a list '{}'".format(join_elements))

    return coll_as_str(flatten(resolve(join_elements, data, data_index)))


def resolve_flatten_template(template, data, data_index):
    flatten_elements = template.get('{flatten}')

    if not flatten_elements:
        raise Exception("Empty '{{flatten}}' template '{}'".format(flatten_elements))
    if not isinstance(flatten_elements, list):
        raise Exception("'{{flatten}}' template is not a list '{}'".format(flatten_elements))

    return flatten(resolve(flatten_elements, data, data_index))


def resolve_or_template(template, data, data_index):
    or_elements = template.get('{or}')

    if not or_elements:
        raise Exception("Empty '{{or}}' template '{}'".format(or_elements))
    if not isinstance(or_elements, list):
        raise Exception("'{{or}}' template is not a list '{}'".format(or_elements))

    def step_function(accumulator, element):
        return accumulator or resolve(element, data, data_index)

    return reduce(step_function, or_elements, False)


def resolve(template, data, data_index):
    if isinstance(template, str):
        return resolve_string_template(template, data, data_index)
    elif isinstance(template, list):
        return list(remove_null(map(partial(resolve, data=data, data_index=data_index), template)))
    elif isinstance(template, dict):
        if '{if}' in template:
            return resolve_if_template(template, data, data_index)
        elif '{or}' in template:
            return resolve_or_template(template, data, data_index)
        elif '{join}' in template:
            return resolve_join_template(template, data, data_index)
        elif '{flatten}' in template:
            return resolve_flatten_template(template, data, data_index)
        else:
            new_dict = dict()
            for (key, value) in template.items():
                new_value = resolve(value, data, data_index)
                if new_value:
                    new_dict[key] = new_value
            return new_dict
    else:
        raise Exception("Unrecognized template '{}'".format(template))
