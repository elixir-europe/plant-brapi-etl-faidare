import itertools
import json
import collections
import lark, lark.lexer
import re
from copy import deepcopy
from functools import reduce, partial

from etl.common.utils import remove_none, get_in, as_list, flatten, distinct, is_list_like, \
    remove_empty

field_template_string_parser = lark.Lark('''
WS: /[ ]/+

LCASE_LETTER: "a".."z"
UCASE_LETTER: "A".."Z"

FIELD: (UCASE_LETTER | LCASE_LETTER)+
field_path: ("." FIELD?)+

object_path: field_path WS* "=>" WS*

value_path: field_path (WS* "+" WS* field_path)*

start: "{" WS* object_path* WS* value_path WS* "}"
''')


def get_field_path(tree):
    return get_in(tree, ['field_path', 'FIELD'])


def tree_to_dict(tree):
    if isinstance(tree, lark.tree.Tree):
        new_children = remove_none(list(map(tree_to_dict, tree.children)))
        return {'{lark}': 'TREE', tree.data: new_children}
    if isinstance(tree, lark.lexer.Token):
        return {'{lark}': 'TOKEN', tree.type: tree.value}
    return tree


def remove_white_space_token(tree):
    if isinstance(tree, lark.tree.Tree):
        def not_ws_token(child):
            if not (isinstance(child, lark.lexer.Token) and child.type == "WS"):
                return child
        without_ws = filter(not_ws_token, tree.children)
        new_children = remove_none(map(remove_white_space_token, without_ws))
        return lark.tree.Tree(tree.data, new_children)
    return tree


def coll_as_str(value, separator=''):
    if isinstance(value, str):
        return value
    if isinstance(value, collections.abc.Iterable):
        return separator.join(map(lambda s: coll_as_str(s, separator), value))
    return str(value)


def capitalize(value):
    if is_list_like(value):
        return list(map(capitalize, value))
    if isinstance(value, str):
        return value.capitalize()
    return value


list_transforms = {"flatten": flatten, "distinct": distinct, "capitalize": capitalize}


def resolve_field_value_template(tree, data, data_index):
    object_paths = as_list(get_in(tree, ['start', 'object_path']))
    for object_path in object_paths:
        field_path = get_field_path(object_path)
        ids = as_list(get_in(data, field_path))
        if not ids:
            return None
        field = field_path[-1]
        entity = re.sub(r"(\w+)URIs?", "\\1", field)
        entity_index = data_index[entity]
        try:
            dataList = []
            for id in ids:
                dataList.append(json.loads(entity_index[id].decode()))
            data = remove_none(dataList)
        except AttributeError:
            data = remove_none(list(map(lambda id: entity_index[id], ids)))

        if getattr(entity_index, 'close', False):
            entity_index.close()
    value_paths = as_list(get_in(tree, ['start', 'value_path']))
    if len(value_paths) == 1:
        return get_in(data, get_field_path(value_paths[0]))
    else:
        new_value = []
        for data in as_list(data):
            field_values = remove_empty(map(
                lambda value_path: as_list(get_in(data, get_field_path(value_path))) or None,
                value_paths
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
        return list(distinct(remove_empty(new_value)))


def resolve_if_template(template, data, data_index):
    if_test, then_branch, else_branch = [template.get(k) for k in ('{if}', '{then}', '{else}')]
    if resolve(if_test, data, data_index):
        return resolve(then_branch, data, data_index)
    elif else_branch:
        return resolve(else_branch, data, data_index)


def resolve_replace_with_template(template, data, data_index):
    parse_study_type_value = parse_template("{.studyTypeName}")
    try:
        study_type_value = resolve(parse_study_type_value, data, data_index)
    except AttributeError:
        # if we have 'studyType' instead of 'studyTypeName' (e.g. VIB)
        parse_study_type_value = parse_template("{.studyType}")
        study_type_value = resolve(parse_study_type_value, data, data_index)
    #study_type_value = study_type_value.lower()

    all_possible_terms = [x.lower() for x in template.get('{replace}')["possible_terms"]]
    replaced_by = template.get('{with}')["replaced_by"]

    if study_type_value is not None and study_type_value in all_possible_terms:
        study_type_value = replaced_by
        return resolve(study_type_value, data, data_index)
    else:
        return


def resolve_join_template(template, data, data_index):
    accept_none = False if template.get('{accept_none}') == False else True
    separator = template.get('{separator}') or ''
    elements = template.get('{join}')
    flattened_elements = flatten(resolve(elements, data, data_index))
    if not accept_none:
        for elem in as_list(flattened_elements):
            if not elem:
                return None
    filtered_elements = remove_none(flattened_elements)
    return coll_as_str(filtered_elements, separator)


def resolve_list_template(template, data, data_index):
    elements = template.get('{list}')
    transform_keys = template.get('{transform}')

    resolved = resolve(as_list(elements), data, data_index)
    if transform_keys:
        for transform_key in transform_keys:
            list_transform = list_transforms[transform_key]
            resolved = list_transform(resolved)
    return list(resolved)


def resolve_or_template(template, data, data_index):
    elements = template.get('{or}')
    initial = False
    return reduce(
        lambda previous, element: previous or resolve(element, data, data_index),
        elements,
        initial
    )


def resolve_map_template(template, data, data_index):
    elements = template.get('{map}')
    transform = template.get('{to}')

    resolved = as_list(resolve(elements, data, data_index))
    if not isinstance(resolved, list):
        raise Exception("Map can only work on lists.")
    if not remove_empty(resolved):
        return None
    resolved = list(map(lambda value: resolve(transform, value, data_index), resolved))
    return resolved


def resolve_merge_template(template, data, data_index):
    merge_branch = template.get('{merge}')
    with_branch = template.get('{with}')

    resolved_merge = resolve(merge_branch, data, data_index)

    resolved_with = resolve(with_branch, data, data_index)

    if not isinstance(resolved_merge, dict) or not isinstance(resolved_with, dict):
        raise Exception("Merge can't work on anything but dictionaries.")
    return merge_dict(resolved_merge, resolved_with)


def resolve(parsed_template, data, data_index):
    if isinstance(parsed_template, str):
        return parsed_template
    elif isinstance(parsed_template, list):
        return list(map(partial(resolve, data=data, data_index=data_index), parsed_template))
    elif isinstance(parsed_template, dict):
        evaluable_templates = {
            '{if}': resolve_if_template,
            '{or}': resolve_or_template,
            '{join}': resolve_join_template,
            '{list}': resolve_list_template,
            '{lark}': resolve_field_value_template,
            '{map}': resolve_map_template,
            '{merge}': resolve_merge_template,
            '{replace}': resolve_replace_with_template
        }
        for key, evaluator in evaluable_templates.items():
            if key in parsed_template:
                return evaluator(parsed_template, data, data_index)

        new_dict = dict()
        for (key, value) in parsed_template.items():
            new_value = resolve(value, data, data_index)
            # added an exception for 'schema:includedInDataCatalog' to keep it as a url
            # This field is linked/should be identical to 'uri' field in the application configuration file
            if new_value and key != "schema:includedInDataCatalog":
                new_dict[key] = new_value
        return new_dict
    else:
        raise Exception("Unrecognized template '{}'".format(parsed_template))


def parse_if_template(template):
    if_test, then_branch, else_branch = [template.get(k) for k in ('{if}', '{then}', '{else}')]

    if not if_test:
        raise Exception("Empty test in '{{if}}' template '{}'".format(template))
    if not then_branch:
        raise Exception("Missing '{{then}}' branch in '{{if}}' template '{}'".format(template))

    return {'{if}': parse_template(if_test),
            '{then}': parse_template(then_branch),
            '{else}': parse_template(else_branch)}


def merge_dict(dict1, dict2):
    merged = deepcopy(dict1)
    merged.update(dict2)
    return merged


def parse_join_template(template):
    elements = as_list(template.get('{join}'))

    if not elements:
        raise Exception("Empty '{{join}}' template '{}'".format(elements))

    return merge_dict(template, {'{join}': parse_template(elements)})


def parse_flatten_template(template):
    elements = template.get('{flatten_distinct}')

    if not elements:
        raise Exception("Empty '{{flatten_distinct}}' template '{}'".format(elements))

    return {'{list}': parse_template(elements), '{transform}': ['flatten', 'distinct']}


def parse_or_template(template):
    elements = template.get('{or}')

    if not elements:
        raise Exception("Empty '{{or}}' template '{}'".format(elements))
    if not isinstance(elements, list):
        raise Exception("'{{or}}' template is not a list '{}'".format(elements))

    return {'{or}': parse_template(elements)}


def parse_string_template(template_string):
    if '{' not in template_string or '}' not in template_string:
        return template_string
    if re.match(r"^{[^{}]+}$", template_string):
        try:
            raw_tree = field_template_string_parser.parse(template_string)
            without_ws = remove_white_space_token(raw_tree)
            dict_tree = tree_to_dict(without_ws)
            return dict_tree
        except lark.lexer.UnexpectedInput as e:
            raise Exception("Could not parse template '{}'".format(template_string), e)
    else:
        tokens = re.findall(r"({[^}]*}|[^{}]+)", template_string)
        return {'{join}': parse_template(tokens), '{accept_none}': False}


def parse_list_template(template):
    elements = template.get('{list}')
    transforms = template.get('{transform}')

    if not elements:
        raise Exception("Empty '{{list}}' template '{}'".format(elements))
    if transforms:
        if not isinstance(transforms, list):
            raise Exception("'{{transform}}' template is not a list '{}'".format(elements))
        for transform in transforms:
            if transform not in list_transforms:
                raise Exception("'{}' is not a valid '{{transform}}' template value".format(transform))

    return {'{list}': parse_template(elements), '{transform}': transforms}


def parse_map_template(template):
    elements = template.get('{map}')
    to = template.get('{to}')

    if not elements:
        raise Exception("Empty '{{map}}' template '{}'".format(elements))
    if not to:
        raise Exception("Missing '{{to}}' branch in '{{map}}' template '{}'".format(template))

    return {'{map}': parse_template(elements), '{to}': parse_template(to)}


def parse_merge_template(template):
    elements = template.get('{merge}')
    with_branch = template.get('{with}')

    if not elements:
        raise Exception("Empty '{{merge}}' template '{}'".format(elements))
    if not with_branch:
        raise Exception("Missing '{{with}}' branch in '{{merge}}' template '{}'".format(template))

    return {'{merge}': parse_template(elements), '{with}': parse_template(with_branch)}


def parse_template(template):
    if isinstance(template, str):
        return parse_string_template(template)
    elif isinstance(template, list):
        return list(remove_none(map(parse_template, template)))
    elif isinstance(template, dict):
        template_parsers = {
            '{if}': parse_if_template,
            '{or}': parse_or_template,
            '{join}': parse_join_template,
            '{flatten_distinct}': parse_flatten_template,
            '{list}': parse_list_template,
            '{map}': parse_map_template,
            '{merge}': parse_merge_template,
        }
        for key, parser in template_parsers.items():
            if key in template:
                return parser(template)

        new_dict = dict()
        for (key, value) in template.items():
            new_value = parse_template(value)
            if new_value:
                new_dict[key] = new_value
        return new_dict
    return template
