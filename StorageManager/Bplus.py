class BTreeNode:
    def __init__(self, order, is_leaf=False):
        self.keys = []  # The keys stored in the node
        self.values = []  # Pointers to the data or "buckets" of pointers
        self.children = []  # Child pointers (used only for internal nodes)
        self.is_leaf = is_leaf  # True if the node is a leaf node
        self.next = None  # Pointer to the next leaf node (for range queries)
        self.order = order  # Maximum number of children

    def is_full(self):
        return len(self.keys) >= self.order - 1


class BPlusTree:
    def __init__(self, order=4):
        self.root = BTreeNode(order, is_leaf=True)
        self.order = order

    def insert(self, key, value):
        """
        Insert a key-value pair into the B+ Tree.
        - `key`: The key to index (e.g., id or name).
        - `value`: Pointer to the row (e.g., row index or memory address).
        """
        result = self._insert_recursive(self.root, key, value)
        if isinstance(result, tuple):  # Root was split
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.keys = [result[0]]
            new_root.children = [self.root, result[1]]
            self.root = new_root

    def _insert_recursive(self, node, key, value):
        if node.is_leaf:
            # Handle insertion in the leaf node
            if key in node.keys:
                # If key already exists, append value (handles secondary indexing)
                index = node.keys.index(key)
                if isinstance(node.values[index], list):
                    node.values[index].append(value)  # Add to the bucket
                else:
                    node.values[index] = [node.values[index], value]  # Convert to a list
            else:
                # Insert new key-value pair
                node.keys.append(key)
                node.values.append(value)
                # Ensure keys are sorted
                sorted_indices = sorted(range(len(node.keys)), key=lambda i: node.keys[i])
                node.keys = [node.keys[i] for i in sorted_indices]
                node.values = [node.values[i] for i in sorted_indices]
        else:
            # Handle insertion in an internal node
            index = self._find_child_index(node, key)
            child_result = self._insert_recursive(node.children[index], key, value)
            
            if isinstance(child_result, tuple):
                node.keys.insert(index, child_result[0])
                node.children.insert(index + 1, child_result[1])

        if len(node.keys) > self.order - 1:
            return self._split_node(node)
        
        return node

    def _split_node(self, node):
        mid = len(node.keys) // 2
        new_node = BTreeNode(self.order, is_leaf=node.is_leaf)

        if node.is_leaf:
            new_node.keys = node.keys[mid:]
            new_node.values = node.values[mid:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]

            # Update next pointers for leaf nodes
            new_node.next = node.next
            node.next = new_node
        else:
            new_node.keys = node.keys[mid + 1:]
            new_node.children = node.children[mid + 1:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]

        return middle_key, new_node

    def _find_child_index(self, node, key):
        for i, k in enumerate(node.keys):
            if key < k:
                return i
        return len(node.keys)

    def search(self, key):
        """
        Search for a key in the B+ Tree and return its associated value(s).
        """
        return self._search_recursive(self.root, key)

    def _search_recursive(self, node, key):
        if node.is_leaf:
            if key in node.keys:
                index = node.keys.index(key)
                return node.values[index]
            return None
        else:
            index = self._find_child_index(node, key)
            return self._search_recursive(node.children[index], key)

    def search_range(self, start, end):
        """
        Search for all keys in the range [start, end] and return their values.
        """
        current_leaf = self.root
        while not current_leaf.is_leaf:
            for i, key in enumerate(current_leaf.keys):
                if start < key:
                    current_leaf = current_leaf.children[i]
                    break
            else:
                current_leaf = current_leaf.children[-1]
        
        result = []
        while current_leaf:
            for i, key in enumerate(current_leaf.keys):
                if start <= key <= end:
                    result.extend(current_leaf.values[i] if isinstance(current_leaf.values[i], list) else [current_leaf.values[i]])
                elif key > end:
                    return result
            current_leaf = current_leaf.next
        
        return result

    def print_tree(self, node=None, prefix="", is_last=True):
        if node is None:
            node = self.root

        print(prefix, end="")
        print("└── " if is_last else "├── ", end="")
        
        node_type = "L" if node.is_leaf else "I"
        print(f"{node_type}: {node.keys} | {node.values if node.is_leaf else ''}")

        if not node.is_leaf:
            new_prefix = prefix + ("    " if is_last else "│   ")
            for i in range(len(node.children)):
                is_last_child = (i == len(node.children) - 1)
                self.print_tree(
                    node=node.children[i], 
                    prefix=new_prefix, 
                    is_last=is_last_child
                )

    def print_leaf_chain(self):
        print("\nLeaf Chain:")
        current_leaf = self.root
        while not current_leaf.is_leaf:
            current_leaf = current_leaf.children[0]
        while current_leaf:
            print(f"[{current_leaf.keys}] → ", end="")
            current_leaf = current_leaf.next
        print("None")


