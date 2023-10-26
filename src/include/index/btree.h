#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p)  ((void)(p))

namespace buzzdb {

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
    struct Node {
        // member to hold the parent node 
        Node *parent; //added 

        /// The level in the tree.
        uint16_t level;

        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count)
            : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }
    };

    struct InnerNode: public Node {
        /// The capacity of a node.
        static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));;

        /// The keys.
        KeyT keys[kCapacity];

        /// The children.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be searched.
        std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
	        for (uint32_t i = 0; i < this->count; ++i) {
                if (keys[i] >= key) {
                    return std::make_pair(i, true);  // Found one!
                }
            }
            // Provided key is greater than all keys in the node.
            return std::make_pair(this->count, false);
        }

        /// Insert a key.
        /// @param[in] key          The separator that should be inserted.
        /// @param[in] split_page   The id of the split page that should be inserted.
        void insert(const KeyT &key, uint64_t split_page) {
 	      // TODO: remove the below lines of code 
    	      // and add your implementation here
	      UNUSED(key);
	      UNUSED(split_page);
           
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // Create a new inner node
            auto *right_inner_node = new (buffer) InnerNode();

            // Create a new key
	        uint32_t split_point = this->count / 2;
            KeyT split_key = keys[split_point - 1];

            right_inner_node->children[0] = children[split_point];
            right_inner_node->count++;
            // TO DO: Still more to do...
	        return split_key;
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
        }

        /// Returns the child page ids.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<uint64_t> get_child_vector() {
            // TODO
	    return std::vector<uint64_t>();
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];

        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            //TODO
            UNUSED(key);
            UNUSED(value);
        }

        /// Erase a key.
        void erase(const KeyT &key) {
            //TODO
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // TODO
            UNUSED(buffer);
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
        }

        /// Returns the values.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<ValueT> get_value_vector() {
            // TODO
        }
    };

    /// The root.
    std::optional<uint64_t> root;

    /// Next page id.
    /// You don't need to worry about about the page allocation.
    /// Just increment the next_page_id whenever you need a new page.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager &buffer_manager)
        : Segment(segment_id, buffer_manager) {
        // TODO
        next_page_id = 1;
    }

    uint64_t find_leaf_node(const KeyT &key){
        //  start with the root node 
        uint64_t current_page_id = root.value_or(0);

        while ( current_page_id != 0 ) {
            BufferFrame &frame = buffer_manager.fix_page(current_page_id, false);
            Node *current_node = (Node *) frame.get_data();

            // we've extracted a node, now we need to determine if it's a leaf or not 
            if (current_node->is_leaf() ) {
                // found a leaf!  Now we gotta do binary search to find the key 
                buffer_manager.unfix_page(frame, false);
                break; 
            } else {
                InnerNode *inner_node = (InnerNode *) current_node;
                auto [index, found] = inner_node->lower_bound(key); 
                if (found )
                    current_page_id = inner_node->children[index];
                else
                    current_page_id = (index > 0) ? inner_node->children[index-1] : 0 ;
            }
        }

        return current_page_id;
    }

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    std::optional<ValueT> lookup(const KeyT &key) {
  
        uint64_t current_page_id = find_leaf_node(key);

        // here leaf should either be found or id is invalid 
        if (current_page_id != 0){
            BufferFrame &frame = buffer_manager.fix_page(current_page_id, false) ; 
            LeafNode *leaf_node = (LeafNode *) frame.get_data() ; 

            // Binary search 
            int low = 0, high = leaf_node->count - 1;
            while (low <= high ) {
                int mid = low + (high - low)/ 2; 
                if (leaf_node->keys[mid] == key) {
                    buffer_manager.unfix_page(frame, false);
                    return leaf_node->values[mid];  
                }
                else if (leaf_node->keys[mid] < key){
                    low = mid + 1;
                }
                else {
                    high = mid - 1;
                }
            }

            buffer_manager.unfix_page(frame, false);
        }

        // means that the key wasn't found!!!  
        return std::optional<ValueT>();

    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT &key) {
        // TODO
	UNUSED(key);
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT &key, const ValueT &value) {
        // TODO
	    // new root
	    if (!root.has_value()) {
	      BufferFrame &frame = buffer_manager.fix_page(
		  buffer_manager.get_overall_page_id(segment_id, next_page_id), true);

	      auto *root_node = new (frame.get_data()) LeafNode();
	      root_node->insert(key, value);

	      buffer_manager.unfix_page(frame, true);
	      next_page_id++;
	      return;
	    }

        uint64_t current_page_id = find_leaf_node(key);
        Node *parent_node = NULL;
        uint64_t parent_page_id = 0;

        // here leaf should either be found or id is invalid 
        if (current_page_id != 0){
            BufferFrame &leaf_frame = buffer_manager.fix_page(current_page_id, true) ; 
            LeafNode *leaf_node = (LeafNode *) leaf_frame.get_data() ; 

            KeyT split_key;
            LeafNode* new_leaf_node = NULL;

            if (leaf_node->count < LeafNode::kCapacity ){
                leaf_node->insert(key, value);
            }
            else {
                BufferFrame &new_leaf_frame = buffer_manager.fix_page(buffer_manager.get_overall_page_id(segment_id, next_page_id), true) ;
                new_leaf_node = new ( new_leaf_frame.get_data()) LeafNode();
                split_key = leaf_node->split((std::byte *)(new_leaf_node));
                buffer_manager.unfix_page(new_leaf_frame, true);
                next_page_id++;
            }

            buffer_manager.unfix_page(leaf_frame, true);

            // now handle the node splits and propagate it up the tree 
		while (parent_node != NULL && parent_node->count == InnerNode::kCapacity ) {
		    BufferFrame &parent_frame = buffer_manager.fix_page(parent_page_id, true);
		    InnerNode * new_inner_node;
		
		    BufferFrame &new_inner_frame = buffer_manager.fix_page(buffer_manager.get_overall_page_id(segment_id, next_page_id), true) ;
		    new_inner_node = new (new_inner_frame.get_data()) InnerNode();
		    KeyT new_split_key = ((InnerNode *) parent_node)->split((std::byte *) new_inner_node );
		
		    // Insert the separator key into the parent node
		    if (parent_node->parent) {
			parent_node->parent->insert(split_key, next_page_id);
		    }
		
		    // now unfix the new inner and parent frames and then update new page id var 
		    buffer_manager.unfix_page(new_inner_frame, true);
		    buffer_manager.unfix_page(parent_frame, true);
		    next_page_id++;
		
		    parent_node = parent_node->parent ; 
		
		    // Update the parent_page_id based on the parent of the current parent_node
		    if (parent_node && parent_node->parent) {
			parent_page_id = parent_node->parent->page_id;
		    } else {
			parent_page_id = root_page_id;
		    }
		
		    split_key = new_split_key;
		    new_leaf_node = NULL;
		}

            //  Does the root also need to be split??  
            if (!parent_node) {
                BufferFrame &new_root_frame = buffer_manager.fix_page(buffer_manager.get_overall_page_id(segment_id, next_page_id), true);
                InnerNode *new_root_node = new (new_root_frame.get_data()) InnerNode();
                new_root_node->keys[0] = split_key;
                new_root_node->children[0] = root.value_or(0) ;
                new_root_node->children[1] = next_page_id - 1;
                new_root_node->count = 1;
                root = next_page_id ;
                buffer_manager.unfix_page(new_root_frame, true) ;
                next_page_id++;
            }
        }
        else {
            // means that the key wasn't found!!!  
            return;
        }

    }

	void erase(const KeyT &key) {
	    // Step 1: Locate the leaf node
	    Node* current_node = root;
	    while (!current_node->is_leaf) {
	        InnerNode* inner = static_cast<InnerNode*>(current_node);
	        uint32_t index = inner->lower_bound(key).first;
	        current_node = inner->children[index]; // Get the child node based on the index
	    }
	    LeafNode* leaf = static_cast<LeafNode*>(current_node);
	
	    // Step 2: Erase from leaf node
	    leaf->erase(key);
	
	    // Step 3: Handle underflow
	    if (leaf->count < LeafNode::kCapacity / 2) {
	        handleUnderflow(leaf);
	    }
	}
	
	void handleUnderflow(LeafNode* leaf) {
	    LeafNode* leftSibling = nullptr;
	    LeafNode* rightSibling = nullptr;
	    InnerNode* parent = leaf->parent;
	
	    // Find siblings
	    uint32_t index;
	    for (index = 0; index < parent->count; ++index) {
	        if (parent->children[index] == leaf) {
	            if (index > 0) leftSibling = static_cast<LeafNode*>(parent->children[index - 1]);
	            if (index < parent->count - 1) rightSibling = static_cast<LeafNode*>(parent->children[index + 1]);
	            break;
	        }
	    }
	
	    // Borrowing from left sibling
	    if (leftSibling && leftSibling->count > LeafNode::kCapacity / 2) {
	        // Borrow the largest key from left sibling
	        leaf->insert(leftSibling->keys[leftSibling->count - 1], leftSibling->values[leftSibling->count - 1]);
	        leftSibling->erase(leftSibling->keys[leftSibling->count - 1]);
	        parent->keys[index - 1] = leaf->keys[0]; // Update separator key in parent
	        return;
	    }
	
	    // Borrowing from right sibling
	    if (rightSibling && rightSibling->count > LeafNode::kCapacity / 2) {
	        // Borrow the smallest key from right sibling
	        leaf->insert(rightSibling->keys[0], rightSibling->values[0]);
	        rightSibling->erase(rightSibling->keys[0]);
	        parent->keys[index] = rightSibling->keys[0]; // Update separator key in parent
	        return;
	    }
	
	    // Merging
	    if (leftSibling) {
	        // Merge with left sibling
	        for (uint32_t i = 0; i < leaf->count; ++i) {
	            leftSibling->insert(leaf->keys[i], leaf->values[i]);
	        }
	        parent->erase(parent->keys[index - 1]);
	        delete leaf; // Free up the memory of the merged leaf node
	    } else if (rightSibling) {
	        // Merge with right sibling
	        for (uint32_t i = 0; i < rightSibling->count; ++i) {
	            leaf->insert(rightSibling->keys[i], rightSibling->values[i]);
	        }
	        parent->erase(parent->keys[index]);
	        delete rightSibling; // Free up the memory of the merged leaf node
	    }
	
	    // Handle potential underflow in parent node
	    if (parent->count < InnerNode::kCapacity / 2) {
	        handleInnerNodeUnderflow(parent);
	    }
	}


        void handleInnerNodeUnderflow(InnerNode* inner) {
	    InnerNode* leftSibling = nullptr;
	    InnerNode* rightSibling = nullptr;
	    InnerNode* parent = inner->parent;
	
	    // If this is the root node and it's empty, handle separately
	    if (!parent) {
	        if (inner->count == 0) {
	            root = inner->children[0];
	            delete inner;
	        }
	        return;
	    }
	
	    // Find siblings
	    uint32_t index;
	    for (index = 0; index < parent->count; ++index) {
	        if (parent->children[index] == inner) {
	            if (index > 0) leftSibling = static_cast<InnerNode*>(parent->children[index - 1]);
	            if (index < parent->count - 1) rightSibling = static_cast<InnerNode*>(parent->children[index + 1]);
	            break;
	        }
	    }
	
	    // Borrowing from left sibling
	    if (leftSibling && leftSibling->count > InnerNode::kCapacity / 2) {
	        // Borrow the largest key and child from left sibling
	        KeyT borrowedKey = parent->keys[index - 1];
	        parent->keys[index - 1] = leftSibling->keys[leftSibling->count - 1];
	        inner->insert(borrowedKey, leftSibling->children[leftSibling->count]);
	        leftSibling->erase(leftSibling->keys[leftSibling->count - 1]);
	        return;
	    }
	
	    // Borrowing from right sibling
	    if (rightSibling && rightSibling->count > InnerNode::kCapacity / 2) {
	        // Borrow the smallest key and child from right sibling
	        KeyT borrowedKey = parent->keys[index];
	        parent->keys[index] = rightSibling->keys[0];
	        inner->insert(borrowedKey, rightSibling->children[0]);
	        rightSibling->erase(rightSibling->keys[0]);
	        return;
	    }
	
	    // Merging
	    if (leftSibling) {
	        // Merge with left sibling
	        leftSibling->insert(parent->keys[index - 1], inner->children[0]);
	        for (uint32_t i = 1; i < inner->count; ++i) {
	            leftSibling->insert(inner->keys[i - 1], inner->children[i]);
	        }
	        parent->erase(parent->keys[index - 1]);
	        delete inner; // Free up the memory of the merged inner node
	    } else if (rightSibling) {
	        // Merge with right sibling
	        inner->insert(parent->keys[index], rightSibling->children[0]);
	        for (uint32_t i = 1; i < rightSibling->count; ++i) {
	            inner->insert(rightSibling->keys[i - 1], rightSibling->children[i]);
	        }
	        parent->erase(parent->keys[index]);
	        delete rightSibling; // Free up the memory of the merged inner node
	    }
	
	    // Handle potential underflow in parent node
	    if (parent->count < InnerNode::kCapacity / 2) {
	        handleInnerNodeUnderflow(parent);
	    }
	}
};

}  // namespace buzzdb
