def sort_dict_lists(dict_to_be_sorted:dict):
    if isinstance(dict_to_be_sorted, list):
        for i in range(len(dict_to_be_sorted)):
            if isinstance(dict_to_be_sorted[i], dict):
                dict_to_be_sorted[i] = sort_dict_lists(dict_to_be_sorted[i])
        return dict_to_be_sorted

    for key, value in dict_to_be_sorted.items():
        if isinstance(value, list) and len(value) > 0 and not isinstance(value[0], dict):
            dict_to_be_sorted[key] = sorted(value)
        elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            for i in range(len(dict_to_be_sorted[key])):
                if isinstance(dict_to_be_sorted[key][i], dict):
                    dict_to_be_sorted[key][i] = sort_dict_lists(dict_to_be_sorted[key][i])
        elif isinstance(value, dict):
            dict_to_be_sorted[key] = sort_dict_lists(value)
    return dict_to_be_sorted
