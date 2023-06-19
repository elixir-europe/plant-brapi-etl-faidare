def sort_dict_lists(dict_to_be_sorted:dict):
    if isinstance(dict_to_be_sorted, list):
        for i in range(len(dict_to_be_sorted)):
            if isinstance(dict_to_be_sorted[i], dict):
                dict_to_be_sorted[i] = sort_dict_lists(dict_to_be_sorted[i])
        return dict_to_be_sorted
    for key, value in dict_to_be_sorted.items():
        if isinstance(value, list):
            dict_to_be_sorted[key] = sorted(value)
        elif isinstance(value, dict):
            dict_to_be_sorted[key] = sort_dict_lists(value)
    return dict_to_be_sorted
