import math

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
    

    def search(self,value):
        return self._search_recursive(self.root, value)
    
    def _search_recursive(self,node : BTreeNode,value):
        if node.is_leaf:
            return value in node.keys
        
        for i, key in enumerate(node.keys):
            if value < key:
                return self._search_recursive(node.children[i],value)
            

        return self._search_recursive(node.children[-1], value)

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
            for value in current_leaf.keys:
                if start <= value <= end:
                    result.append(value)
                elif value > end:
                    return result
            current_leaf = current_leaf.next
        
        return result

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
    
    def _find_child_index_deletion(self, node, value):
        for i, key in enumerate(node.keys):
            if value == key : 
                return i, 
            if value < key:
                return i
        return len(node.keys)

    def _smallest_leaf_node_on_the_right_subtree(self, node) :
        if not node.is_leaf :
            if self.root : 
                self._smallest_leaf_node_on_the_right_subtree(node.children[1])
            else : 
                self._smallest_leaf_node_on_the_right_subtree(node.children[0])
        return node.keys[0]

    def _find_successor(self, node, idx):
        current = node.children[idx]
        while not current.is_leaf:
            current = current.children[0]
        return current.keys[1]

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





# def main():
#     tree = BPlusTree(order=4)
    
#     values = [10, 20, 5, 15, 25, 30, 8, 12, 7, 18, 22, 35, 40, 50, 55, 60,33,56, 11, 19, 13, 57,58, 14,6,36,37,38,39]
    
#     for i, value in enumerate(values):
#         print(f"\nInserting {value}")
#         tree.insert(value)
        
#         print("\nTree Structure:")
#         tree.print_tree()
        
#         tree.print_leaf_chain()

#         if (i > 5) : 
#             print(tree.root.children)
        
#         print("\n" + "-" * 50)

#     print(tree.search_range(10,20))

#     tree.delete(60)
#     tree.delete(58)
#     tree.print_tree()

# if __name__ == "__main__":
#     main()