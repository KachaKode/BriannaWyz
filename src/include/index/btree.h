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
        static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));

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
        void insert(const KeyT &key, uint64_t split_page) {  // *MarcAdd*
 	    // Find the correct position to insert the new key
	    int index = 0;
	    while (index < count && keys[index] < key) {
	        index++;
	    }
	
	    // Shift keys and children to the right to make space for the new key and child
	    for (int i = count; i > index; i--) {
	        keys[i] = keys[i - 1];
	        children[i + 1] = children[i];
	    }
	
	    // Insert the new key and child
	    keys[index] = key;
	    children[index + 1] = child;
	
	    // Increase the count of keys in the node
	    count++;
           
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
            
            for (int i = split_point; i < this->count; i++){
                right_inner_node->keys[i - split_point] = keys[i];
                right_inner_node->children[i - split_point + 1] = children[i + 1];  //*MarcAdd*
            }

            // update the counts  of each node 
            right_inner_node->count = this->count - split_point;
            this->count = split_point;

            return split_key;
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            return keys;
        }

        /// Returns the child page ids.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<uint64_t> get_child_vector() {
            return children;
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));

        /// The keys.
        KeyT keys[kCapacity];

        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {  //  *MarcAdd*
            // Find the correct position to insert the new key
	    uint32_t index = 0;
	    while (index < count && keys[index] < key) {
	        index++;
	    }
	
	    // Shift keys and values to the right to make space for the new key and value
	    for (uint32_t i = count; i > index; --i) {
	        keys[i] = keys[i - 1];
	        values[i] = values[i - 1];
	    }
	
	    // Insert the new key and value
	    keys[index] = key;
	    values[index] = value;
	
	    // Increase the count of keys and values in the node
	    count++;
        }

        /// Erase a key.
        void erase(const KeyT &key) {
            // find the key in the leaf's key array 
            uint16_t index = static_cast<uint16_t>(-1);;
            for (uint16_t i = 0; i < this->count; ++i) {
                if (this->keys[i] == key) {
                    index = i;
                    break;  // Key found
                }
            }
            if (index != static_cast<uint16_t>(-1)) {
                // Key found; remove it
                for (uint16_t i = index; i < this->count - 1; ++i) {
                    keys[i] = keys[i + 1];
                    values[i] = values[i + 1];
                }
                this->count--;
            } 
            return;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // Create a new inner node
            auto *right_leaf_node = new (buffer) LeafNode();

            // Create a new key
	        uint32_t split_point = this->count / 2;

            
            for (int i = split_point; i < this->count; i++){
                right_leaf_node->keys[i - split_point] = keys[i];
                right_leaf_node->values[i - split_point] = values[i];
            }

            // update the counts  of each node 
            right_leaf_node->count = this->count - split_point;
            this->count = split_point;

            return right_leaf_node->keys[0];
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            return keys; 
        }

        /// Returns the values.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<ValueT> get_value_vector() {
            return values;
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
                if (found)
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
        // first we gotta find the leaf node 
        uint64_t leaf_page_id = find_leaf_node(key);

        BufferFrame &frame = buffer_manager.fix_page(leaf_page_id, false) ; 
        LeafNode *leaf_node = (LeafNode *) frame.get_data(); 
        leaf_node->erase(key);
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
                KeyT new_split_key = ((InnerNode *) parent_node)->split((std::byte *) new_inner_node); 

		// Insert the separator key into the parent node  *MarcAdd*
	        if (parent_node->parent) {
		    parent_node->parent->insert(split_key, next_page_id);
	        }

                // now unfix the new inner and parent frames and then upda new page id var 
                buffer_manager.unfix_page(new_inner_frame, true);
                buffer_manager.unfix_page(parent_frame, true);
                next_page_id++;

                parent_node = parent_node->parent ; 
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
};

}  // namespace buzzdb
