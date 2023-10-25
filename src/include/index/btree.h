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
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];

        /// The children.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be searched.
        std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
	      // TODO: remove the below lines of code 
    	      // and add your implementation here
            return std::pair<uint32_t, bool>();
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
            // TODO: remove the below lines of code 
    	      // and add your implementation here
		UNUSED(buffer);
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

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    std::optional<ValueT> lookup(const KeyT &key) {
        // TODO
	UNUSED(key);
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
      UNUSED(key);
      UNUSED(value);
    }
};

}  // namespace buzzdb
