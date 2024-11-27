import math

class BTreeNode:
    def __init__(self, order, is_leaf=False):
        self.keys = []  # The keys stored in the node
        self.values = []  # Pointers to the data or "buckets" of pointers , bucker if it is secondary index
        self.children = []  # Child pointers used only for internal nodes
        self.is_leaf = is_leaf  # True if the node is a leaf node
        self.next = None  # Pointer to the next leaf node (for range queries)
        self.order = order  # Maximum number of children
        self.parent = None
        self.min_children = math.ceil(order / 2) 
        self.min_key = math.ceil(order / 2) - 1

    def is_full(self):
        return len(self.keys) >= self.order - 1


class BPlusTree:
    def __init__(self, order=4):
        self.root = BTreeNode(order, is_leaf=True)
        self.order = order
        
    def insert(self, key, value):
        result = self._insert_recursive(self.root, key, value)
        if isinstance(result, tuple):  # Root was split
            middle_key, new_node = result
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.keys = [middle_key]
            new_root.children = [self.root, new_node]

            # Update parent references
            self.root.parent = new_root
            new_node.parent = new_root

            self.root = new_root


    def _insert_recursive(self, node, key, value):
        if node.is_leaf:
            if key in node.keys:
                index = node.keys.index(key)
                if isinstance(node.values[index], list):
                    node.values[index].append(value)
                else:
                    node.values[index] = [node.values[index], value]
            else:
                node.keys.append(key)
                node.values.append(value)

                # Ensure keys and values remain sorted
                sorted_indices = sorted(range(len(node.keys)), key=lambda i: node.keys[i])
                node.keys = [node.keys[i] for i in sorted_indices]
                node.values = [node.values[i] for i in sorted_indices]
        else:
            index = self._find_child_index(node, key)
            child_result = self._insert_recursive(node.children[index], key, value)

            if isinstance(child_result, tuple):
                middle_key, new_node = child_result
                node.keys.insert(index, middle_key)
                node.children.insert(index + 1, new_node)

                # Update parent references
                new_node.parent = node
                for child in node.children:
                    child.parent = node

        if len(node.keys) > self.order - 1:
            return self._split_node(node)

        return node


    def _split_node(self, node):
        mid = len(node.keys) // 2
        new_node = BTreeNode(self.order, is_leaf=node.is_leaf)

        if node.is_leaf:
            # Split leaf node
            new_node.keys = node.keys[mid:]
            new_node.values = node.values[mid:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]

            # Update leaf chain
            new_node.next = node.next
            node.next = new_node
        else:
            # Split internal node
            new_node.keys = node.keys[mid + 1:]
            new_node.children = node.children[mid + 1:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]

            # Update parent for new_node's children
            for child in new_node.children:
                child.parent = new_node

        # Set parent for the new node
        new_node.parent = node.parent

        return middle_key, new_node


    def _find_child_index(self, node, key):
        for i, k in enumerate(node.keys):
            if key < k:
                return i
        return len(node.keys)

    def search(self, key):
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


        
    # Other methods (insert, _insert_recursive, etc.) remain unchanged
    def delete(self, value):
        if not self.root:
            return

        self._delete_recursive(self.root, value)

        # If the root becomes empty and is not a leaf, promote its only child
        if not self.root.keys and not self.root.is_leaf:
            self.root = self.root.children[0]

    def _delete_recursive(self, node, value):
        if node.is_leaf:
            # Case: Delete from leaf node
            if value in node.keys:
                node.keys.remove(value)
        else:
            # Case: Delete from internal node
            idx = self._find_child_index(node, value)
            
            if value in node.keys:
                # Replace the key with its successor or predecessor
                print("idx ditemukannya value sama =", idx)
                successor = self._find_successor(node, idx)
                print("successor =", successor)
                
                current = node.children[idx]
                while (not current.is_leaf) : 
                    current = current.children[0]

                print("panjang node children =", len(current.keys))
                print("isi node: ")
                for all in current.keys :
                    print(all)
                if len(current.keys) > node.min_key :
                    node.keys[idx - 1] = successor
                    # print("jumlah keys")
                    print("node keys baru =", node.keys[idx - 1])
                    self._delete_recursive(node.children[idx], value)
            else:
                # Continue searching in the appropriate child
                self._delete_recursive(node.children[idx], value)

        # Handle underflow
        if len(node.keys) < (self.order // 2) - 1:
            self._handle_underflow(node)


    def _find_successor(self, node, idx):
        current = node.children[idx]
        while not current.is_leaf:
            current = current.children[0]
        return current.keys[1]

    def _smallest_leaf_node_on_the_right_subtree(self, node) :
        if not node.is_leaf :
            if self.root : 
                self._smallest_leaf_node_on_the_right_subtree(node.children[1])
            else : 
                self._smallest_leaf_node_on_the_right_subtree(node.children[0])
        return node.keys[0]
    
    def _fix_underflow(self, parent, index):
        child = parent.children[index]
        
        # Check for a left sibling to borrow/merge
        if index > 0:
            left_sibling = parent.children[index - 1]
            if len(left_sibling.keys) > (self.order - 1) // 2:
                self._borrow_from_left(parent, index, left_sibling)
            else:
                self._merge_with_left(parent, index, left_sibling)
        # Check for a right sibling to borrow/merge
        elif index < len(parent.children) - 1:
            right_sibling = parent.children[index + 1]
            if len(right_sibling.keys) > (self.order - 1) // 2:
                self._borrow_from_right(parent, index, right_sibling)
            else:
                self._merge_with_right(parent, index, right_sibling)

    def _borrow_from_left(self, parent, index, left_sibling):
        child = parent.children[index]
        borrowed_key = left_sibling.keys.pop(-1)
        
        if child.is_leaf:
            child.keys.insert(0, borrowed_key)
            parent.keys[index - 1] = borrowed_key
        else:
            borrowed_child = left_sibling.children.pop(-1)
            child.keys.insert(0, parent.keys[index - 1])
            child.children.insert(0, borrowed_child)
            parent.keys[index - 1] = borrowed_key

    def _borrow_from_right(self, parent, index, right_sibling):
        child = parent.children[index]
        borrowed_key = right_sibling.keys.pop(0)
        
        if child.is_leaf:
            child.keys.append(borrowed_key)
            parent.keys[index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        else:
            borrowed_child = right_sibling.children.pop(0)
            child.keys.append(parent.keys[index])
            child.children.append(borrowed_child)
            parent.keys[index] = borrowed_key

    def _merge_with_left(self, parent, index, left_sibling):
        child = parent.children[index]
        
        if child.is_leaf:
            left_sibling.keys.extend(child.keys)
            left_sibling.next = child.next
        else:
            left_sibling.keys.append(parent.keys[index - 1])
            left_sibling.keys.extend(child.keys)
            left_sibling.children.extend(child.children)
        
        parent.keys.pop(index - 1)
        parent.children.pop(index)

    def _merge_with_right(self, parent, index, right_sibling):
        child = parent.children[index]
        
        if child.is_leaf:
            child.keys.extend(right_sibling.keys)
            child.next = right_sibling.next
        else:
            child.keys.append(parent.keys[index])
            child.keys.extend(right_sibling.keys)
            child.children.extend(right_sibling.children)
        
        parent.keys.pop(index)
        parent.children.pop(index + 1)





def main():
    tree = BPlusTree(order=4)
    
    values = [[10, "A"], [20, "B"], [5, "C"], [15, "D"], [25, "E"], [30, "F"], [8, "G"], [12, "H"], [7, "I"], [18, "J"], [22, "K"], [35, "L"], [40, "M"], [50, "N"], [55, "O"], [60, "P"], [33, "Q"], [56, "R"], [11, "S"], [19, "T"], [13, "U"], [57, "V"], [58, "W"], [14, "X"], [6, "Y"], [36, "Z"], [37, "AA"], [38, "BB"], [39, "CC"]]
    
    for key, value in values:
        print(f"\nInserting {value}")
        tree.insert(key, value)
        
        print("\nTree Structure:")
        tree.print_tree()
        
        tree.print_leaf_chain()
        
        print("\n" + "-" * 50)

    print(tree.search_range(10,20))

    # tree.delete(60)
    tree.delete(56)
    tree.print_tree()

if __name__ == "__main__":
    main()