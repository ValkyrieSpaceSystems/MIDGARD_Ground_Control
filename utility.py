

def get_element(data_tree, key, default=None):
    current = data_tree
    for key_part in key.split('.'):
        if isinstance(current, dict) and key_part in current:
            current = current[key_part]
        else:
            return default
    #print(current)
    return current

def combine_with_and(items, oxford_comma=True):
    items = list(items)

    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"

    *head, last = items
    sep = "," if oxford_comma else ""
    return f"{', '.join(head)}{sep} and {last}"
    
def label(raw):
    return raw.replace('_', ' ').title()