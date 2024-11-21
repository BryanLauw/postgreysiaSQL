class BTreeNode:
    def __init__(self, order, is_leaf=False):
        self.keys = []
        self.children = []
        self.is_leaf = is_leaf
        self.next = None
        self.order = order

    def is_full(self):
        return len(self.keys) >= self.order - 1


class BPlusTree:
    def __init__(self, order=4):
        self.root = BTreeNode(order, is_leaf=True)
        self.order = order

    def insert(self, value):
        result = self._insert_recursive(self.root, value)
        if isinstance(result, tuple):
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.keys = [result[0]]
            new_root.children = [self.root, result[1]]
            self.root = new_root

    def _insert_recursive(self, node, value):
        if node.is_leaf:
            node.keys.append(value)
            node.keys.sort()
        else:
            index = self._find_child_index(node, value)
            child_result = self._insert_recursive(node.children[index], value)
            
            if isinstance(child_result, tuple):
                node.children.insert(index + 1, child_result[1])
                node.keys.insert(index, child_result[0])

        if len(node.keys) > self.order - 1:
            return self._split_node(node)
        
        return node

    def _split_node(self, node : BTreeNode):
        mid = len(node.keys) // 2
        
        new_node = BTreeNode(self.order, is_leaf=node.is_leaf)

        if node.is_leaf:
            new_node.keys = node.keys[mid:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]

            new_node.next = node.next
            node.next = new_node
        else:
            new_node.keys = node.keys[mid+1:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            
            
            new_node.children = node.children[mid+1:]
            node.children = node.children[:mid+1]

        return (middle_key, new_node)

    def _find_child_index(self, node, value):
        for i, key in enumerate(node.keys):
            if value < key:
                return i
        return len(node.keys)

    def print_tree(self, node=None, prefix="", is_last=True):
        if node is None:
            node = self.root

        print(prefix, end="")
        print("└── " if is_last else "├── ", end="")
        
        node_type = "L" if node.is_leaf else "I"
        print(f"{node_type}: {node.keys}")

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
            print(f"[{current_leaf.keys}]", end=" → ")
            current_leaf = current_leaf.next
        print("None")


def main():
    tree = BPlusTree(order=5)
    
    values = [10, 20, 5, 15, 25, 30, 8, 12, 7, 18, 22, 35, 40, 50, 55, 60]
    
    for value in values:
        print(f"\nInserting {value}")
        tree.insert(value)
        
        print("\nTree Structure:")
        tree.print_tree()
        
        tree.print_leaf_chain()
        
        print("\n" + "-" * 50)


if __name__ == "__main__":
    main()