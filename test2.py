master_array = {
    # ✅ A: self-contradiction (impossible)
    "A": {
        "state": False,
        "conditions_true": ["A"],      # depends on itself being True
        "conditions_false": ["B"],     # also depends on B being False
    },

    # ✅ B: direct contradiction with C
    "B": {
        "state": False,
        "conditions_true": ["C"],      # wants C True
        "conditions_false": [],        # fine
    },
    "C": {
        "state": False,
        "conditions_true": [],
        "conditions_false": ["B"],     # wants B False (contradiction with B)
    },

    # ✅ D: circular true dependency with E
    "D": {
        "state": False,
        "conditions_true": ["E"],
        "conditions_false": [],
    },
    "E": {
        "state": False,
        "conditions_true": ["D"],
        "conditions_false": [],
    },

    # ✅ F: indirect deadlock through G and H
    "F": {
        "state": False,
        "conditions_true": ["G"],      # F needs G True
        "conditions_false": [],
    },
    "G": {
        "state": False,
        "conditions_true": ["H"],      # G needs H True
        "conditions_false": [],
    },
    "H": {
        "state": False,
        "conditions_true": [],
        "conditions_false": ["F"],     # H needs F False → circular deadlock
    },

    # ✅ J: redundant entries (duplicate condition)
    "J": {
        "state": False,
        "conditions_true": ["K", "K"],  # repeated K
        "conditions_false": [],
    },
    "K": {
        "state": False,
        "conditions_true": [],
        "conditions_false": [],
    },
}

# , both true, both inverted, path dependence failure, duplicates



def check_incompatibilities():
    issues = []
    list_of_issues = []

    for switch in master_array:
        conditions_true = master_array[switch]['conditions_true']
        conditions_false = master_array[switch]['conditions_false']
        all_conditions = conditions_true + conditions_false
        
        # Check for self in conditions
        if switch in conditions_true: 
            issues.append(f"KeyError: {switch} in own conditions_true")
            list_of_issues.append((switch))
        if switch in conditions_false: 
            issues.append(f"KeyError: {switch} in own conditions_false")
            list_of_issues.append((switch))
            
        # Check for duplicate conditions
        if len(conditions_true) != len(set(conditions_true)): 
            issues.append(f'KeyError: {switch} has duplicate conditions {', '.join([cond for cond in set(conditions_true) if conditions_true.count(cond) > 1])} in conditions_true')
        if len(conditions_false) != len(set(conditions_false)): 
            issues.append(f'KeyError: {switch} has duplicate conditions {', '.join([cond for cond in set(conditions_false) if conditions_false.count(cond) > 1])} in conditions_false')
        
        # Check for same condition in both conditions_true and conditions_false
        for cond in set(all_conditions):
            if cond != switch: # Ignore self in conditions
                if cond in conditions_true and cond in conditions_false: 
                    issues.append(f'KeyError: {switch} has condition {cond} in both conditions_true and conditions_false')
                    
        # Check for mutually true
        for cond in conditions_true:
            if cond != switch and not any({switch,cond} <= set(t) for t in list_of_issues): # Ignore self in conditions and already detected issues
                if switch in master_array[cond]['conditions_true']:
                    issues.append(f"KeyError: {switch} and {cond} both have each other in conditions_true")
                    list_of_issues.append((switch, cond))
                
    # Check for self shutoff
    graph = {}
    for switch, data in master_array.items():
        graph[switch] = [(c, True) for c in data.get("conditions_true", [])] + [(c, False) for c in data.get("conditions_false", [])]

    visited = set()
    path = []

    def dfs(switch):
        if switch in path:
            cycle = path[path.index(switch):] + [switch]
            # Build a readable representation of dependency types in the cycle
            arrows = []
            for i in range(len(cycle) - 1):
                source = cycle[i]
                destination = cycle[i + 1]
                # find polarity of this edge
                for (c, polarity) in graph[source]:
                    if c == destination:
                        arrows.append(f"{source} needs {destination} {'True' if polarity else 'False'}")
                        break
                    
            if not any({switch,c} <= set(t) for t in list_of_issues):
                issues.append(f"KeyError: {switch} deactuates itself: " + " -> ".join(arrows))
            return
        if switch in visited:
            return
        visited.add(switch)
        path.append(switch)
        for next, _ in graph.get(switch, []):
            if next in master_array:  # skip unknowns already logged
                dfs(next)
        path.pop()

    for switch in master_array:
        dfs(switch)
        
    if issues != []:
        print(list_of_issues)
        raise Exception("\n"+"\n".join(issues))



print(check_incompatibilities())