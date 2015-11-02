/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#define ALWAYS_INLINE __attribute__ ((always_inline))
#define HOT __attribute__ ((__hot__))
#define PACKED __attribute__ ((__packed__))

#include "Math.h"
#include <boost/static_assert.hpp>
#include <algorithm>
#include <cassert>
#include <cstddef>

namespace rtree {

/*!
 * \brief A cache-conscious, extremely space efficient, packed, static, 
 * 1-dimensional R-Tree.
 * 
 * This type of index allows "stab queries" on 1-dimensional elements (ranges).
 * 
 * All memory is pre-allocated for maximum cache locality. By defining the
 * maximum size at compile time (via template parameters), the offset
 * calculations and loops can be heavily optimized by the compiler.
 * 
 * Inspired by http://lin-ear-th-inking.blogspot.com/2007/06/packed-1-
 *   dimensional-r-tree.html
 * 
 * \tparam T							Element type.
 * \tparam Key							Key type (2 keys define a range).
 * \tparam IM_FAN_OUT					Fan-out of intermediate nodes.
 * \tparam LEAF_FAN_OUT					Fan-out of leaf nodes.
 * \tparam SIZE							Maximum number of elements.
 */ 
template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
>
class OneDimRTree {
	
public:
	
	/*!
	 * \brief Definition of a range.
	 */
	typedef Key KeyRange[2];
	
	/*!
	 * \brief Maximum number of elements.
	 */
	static const unsigned short size = SIZE;
	
	/*!
	 * \brief The type of the keys.
	 */
	typedef Key KeyType;
	
	/*!
	 * \brief The type of the elements.
	 */
	typedef T ElementType;
	
	/*!
	 * \brief Constructor.
	 */
	OneDimRTree();

#ifdef DEBUG
	/*!
	 * \brief Constructor.
	 */
	~OneDimRTree();
#endif	
	
	/*!
	 * \brief Replaces the contents of the OneDimRTree with the given elements 
	 * and bounds.
	 * 
	 * \param elements					The elements to put in the tree.
	 * \param bounds					Bounds of the elements, 2 for each 
	 * 									element.
	 * \param noOfElements				Number of elements (i.e. half the number 
	 * 									of bounds).
	 */
	void reset(
		const T *elements,
		const KeyRange bounds[],
		unsigned short noOfElements
	);
	
	/*!
	 * \brief Get all elements with the given key within their bounds (stab
	 * query) and write them to the found array.
	 * 
	 * \param key				The key to "stab" the index with.
	 * \param[out] found		The elements.
	 * 
	 * \return 					The number of elements written to the found
	 * 							array.
	 */
	inline unsigned short find(
		Key key,
		T *found
	) const HOT;
	
	/*!
	 * \brief Iterate over all elements with the given within their bounds (stab
	 * query) and call visitor::operator() on them.
	 * 
	 * \tparam Visitor			The visitor type.
	 * \param key				The key to "stab" the index with.
	 * \param visitor			The visitor.
	 */
	template <typename Visitor>
	inline void visit(
		Key key,
		Visitor &visitor
	) const HOT;
	
	/*!
	 * \brief Get the number of elements in the tree.
	 * 
	 * \return 					The number of elements in the tree. 
	 */ 
	inline unsigned short getNoOfElements() const throw();
	
private:
	
	/*!
	 * \brief An entry in the tree.
	 */
	struct Entry {
		
		/*!
		 * \brief The bounds (range) of the element.
		 */
		KeyRange m_bounds;
		
		/*!
		 * \brief The element.
		 */
		T m_element;
		
    } PACKED;
	
	/*!
	 * \brief A leaf node of the tree.
	 * 
	 * Leaf nodes contain the actual entries, which are elements and their
	 * bounds (range).
	 */
	struct LeafNode {
		
		/*!
		 * \brief The entries of the leaf node. 
		 */
		Entry m_entries[LEAF_FAN_OUT];
		
    } PACKED;
	
	/*!
	 * \brief An intermediate node of the tree.
	 * 
	 * Intermediate nodes contain no entries, but only the bounds of each
	 * child node. Since this is an R-Tree, the bounds of a node are defined
	 * as the union of the bounds of its of its contents.
	 * 
	 * Note that intermediate nodes have no pointers to their children.
	 * Because the size of the tree is known at compile time and all memory
	 * is pre-allocated, traversal of the tree is possible by memory-offset
	 * calculations only. 
	 */
	struct IntermediateNode {
		
		/*!
		 * \brief The bounds (range) of the child nodes.
		 */
		KeyRange m_bounds[IM_FAN_OUT];
		
    } PACKED;
	
	/*!
	 * \brief Forward declaration.
	 */
	struct Center;
	
	/*!
	 * \brief The number of leaf nodes in the tree.
	 */
	static const unsigned short LEAF_NODES = 
		(SIZE + LEAF_FAN_OUT - 1) / LEAF_FAN_OUT;
	
	/*!
	 * \brief Get all elements with the given key within their bounds (stab
	 * query) and write them to the found array.
	 * 
	 * This is the private, recursive implementation of the public
	 * find(Key, T*).
	 * 
	 * \param key				The key to "stab" the index with.
	 * \param[out] found		The elements.
	 * \param n					The index of the node of the tree. Up to
	 * 							IM_NODES, the node. Beyond, we know it is a leaf
	 * 							node.
	 * 
	 * \return 					The number of elements written to the found
	 * 							array.
	 * 
	 * \sa find(Key, T*)
	 */
	inline unsigned short find(
		Key key,
		T *found,
		unsigned short n
	) const;
	
	/*!
	 * \brief Iterate over all elements with the given within their bounds (stab
	 * query) and call visitor::operator() on them.
	 *
	 * This is the private, recursive implementation of the public
	 * visit(Key, Visitor&).
	 * 
	 * \tparam Visitor			The visitor type.
	 * \param key				The key to "stab" the index with.
	 * \param visitor			The visitor.
	 * \param n					The index of the node of the tree. Up to
	 * 							IM_NODES, the node. Beyond, we know it is a leaf
	 * 							node.
	 * 
	 * \sa visit(Key, Visitor&)
	 */
	template <typename Visitor>
	inline void visit(
		Key key,
		Visitor &visitor,
		unsigned short n
	) const;
	
	/*!
	 * \brief The number of intermediate levels.
	 */
	static const unsigned short IM_LEVELS = 
		Log<LEAF_NODES, IM_FAN_OUT>::value;
	
	/*!
	 * \brief The number of intermediate nodes.
	 */
	static const unsigned short IM_NODES =
		PowSum<IM_FAN_OUT, IM_LEVELS>::value;
	
	/*!
	 * \brief The number of virtual intermediate nodes.
	 * 
	 * These nodes do not actually exist. However, "imagining" them behind the
	 * real intermediate nodes allows for a very efficient, unified offset 
	 * calculation across both intermediate and leaf nodes, without corner
	 * cases.
	 */
	static const unsigned short VIRTUAL_IM_NODES =
		Pow<IM_FAN_OUT, IM_LEVELS>::value - 
			(LEAF_NODES + IM_FAN_OUT - 1) / IM_FAN_OUT;
	
	/*!
	 * \brief The number of virtual intermediate nodes can never be larger
	 * than the number of intermediate nodes. 
	 */
	BOOST_STATIC_ASSERT(VIRTUAL_IM_NODES <= IM_NODES); 
	
	/*!
	 * \brief The number of elements stored in the tree.
	 */
	unsigned short m_noOfElements;
	
	/*!
	 * \brief The index of the first node in use. This is the entry point for
	 * find() and visit().
	 * 
	 * The tree has a static maximum size. However, we do not require the
	 * user to fill it up completely at runtime. When the tree is almost empty,
	 * traversing the tree from the root node would be a waste of time. Instead,
	 * we bulk-load the tree from the bottom-left corner and specify an entry
	 * node, so we can immediately skip all unused nodes.
	 */
	unsigned short m_entryNode;
	
	/*!
	 * \brief The intermediate nodes.
	 * 
	 * Note that this is just an array of nodes. Traversal is done only via
	 * offset calculations.
	 */
	IntermediateNode m_imNodes[IM_NODES - VIRTUAL_IM_NODES];
	
	/*!
	 * \brief The leaf nodes.
	 * 
	 * Note that this is just an array of nodes. Traversal is done only via
	 * offset calculations.
	 */
	LeafNode m_leafNodes[LEAF_NODES];
	
};

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::OneDimRTree()
:	m_noOfElements(0) {
	this->reset(0, 0, 0);
}

#ifdef DEBUG
template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::~OneDimRTree() {
	
	unsigned short leaves =
		(this->m_noOfElements + LEAF_FAN_OUT - 1) / LEAF_FAN_OUT;
	assert(leaves < LEAF_NODES);
	unsigned short imNodes = 0;
	if (this->m_noOfElements > 0) {
		int l = leaves;
		while (l >= IM_FAN_OUT)
			l /= LEAF_FAN_OUT;
		l = 1;
		int log = 1;
		for (int i = 0; i < log; ++i) {
			imNodes += l;
			l *= LEAF_FAN_OUT;
		}
	}
	assert(imNodes <= IM_NODES);
	
	const size_t totalSize = sizeof(LeafNode) * leaves +
		sizeof(IntermediateNode) * imNodes;
	util::msglog::CompositeMessageLog::getInstance()->issue(
		"Crescando:RTree", util::msglog::MessageLog::LOG__TRACE,
		"size of indexing data structure (rtree): %d", totalSize);
	
}
#endif

/*!
 * \brief The center of a range (pair of keys).
 * 
 * We need some way of totally ordering all entries in the tree (for bulk-
 * loading), and picking the center of the bounds is as good as it gets.
 */
template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> struct OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::Center {
	
	/*!
	 * \brief The value of the center.
	 * 
	 * The type of this is Key, since the center is a point, and points are
	 * search keys of this index structure.
	 */
	Key m_value;
	
	/*!
	 * \brief The index of the center.
	 * 
	 * This allows one to sort a set of Center objects and remember their
	 * original order.
	 */
	unsigned short m_index;
	
	/*!
	 * \brief Less-than operator.
	 * 
	 * \param other			The other center to compare with.
	 */
	inline bool operator<(const Center &other) const throw() {
		return this->m_value < other.m_value;
	}
	
	/*!
	 * \brief Equality operator.
	 * 
	 * \param other			The other center to compare with.
	 */
	inline bool operator==(const Center &other) const throw() {
		return this->m_value == other.m_value;
	}
};

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> void OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::reset(
	const T * const elements,
	const KeyRange bounds[],
	const unsigned short noOfElements
) {
	
	// store size
	assert(noOfElements <= SIZE);
	this->m_noOfElements = noOfElements;
	
	// sort entries by center of bounds, in ascending order
	Center centers[SIZE];
	for (unsigned short i = 0; i < noOfElements; ++i) {
		const Key &lb = bounds[i][0];
		const Key &ub = bounds[i][1];
		centers[i].m_index = i;
		centers[i].m_value = (lb + ub) / 2;
	}
	std::sort(&centers[0], &centers[noOfElements]);
	
	// bulk-load the leaf nodes with the sorted entries, starting in the
	// bottom-left corner of the tree, i.e. the leftmost leaf node
	for (unsigned short i = 0; i < noOfElements; ++i) {
		const unsigned short index = centers[i].m_index;
		assert(index < noOfElements);
		const T &element = elements[index];
		assert(i / LEAF_FAN_OUT < LEAF_NODES);
		LeafNode &leafNode = this->m_leafNodes[i / LEAF_FAN_OUT];
		Entry &entry = leafNode.m_entries[i % LEAF_FAN_OUT];
		entry.m_element = element;
		entry.m_bounds[0] = bounds[index][0];
		entry.m_bounds[1] = bounds[index][1];
	}
	
	// pad unused leaf entries with illegal bounds
	for (unsigned short i = noOfElements; i % LEAF_FAN_OUT; ++i) {
		assert(i / LEAF_FAN_OUT < LEAF_NODES);
		LeafNode &leafNode = this->m_leafNodes[i / LEAF_FAN_OUT];
		Entry &entry = leafNode.m_entries[i % LEAF_FAN_OUT];
		entry.m_bounds[0] = max<Key>();
		entry.m_bounds[1] = min<Key>();
	}
	
    const unsigned short noOfLeafs = static_cast<unsigned short>
            ((noOfElements + LEAF_FAN_OUT - 1) / LEAF_FAN_OUT);
	assert(noOfLeafs <= LEAF_NODES);
	
	// check for the special case of a single-leaf or empty tree
	if (noOfLeafs <= 1) {
		
		// special case found; entry node is the leaf node itself
		this->m_entryNode = IM_NODES;
		
		// check for special case of no entries at all
		if (!noOfLeafs) {
			
			// special case found; format the empty leaf node
			for (unsigned short i = 0; i < LEAF_FAN_OUT; ++i) {
				BOOST_STATIC_ASSERT(LEAF_NODES >= 1);
				LeafNode &leafNode = this->m_leafNodes[0];
				Entry &entry = leafNode.m_entries[i];
				entry.m_bounds[0] = max<Key>();
				entry.m_bounds[1] = min<Key>();
			}
			
		}
		
		// we are done
		return;
		
	}
	
	// propagate bounds from leaf nodes to lowest-level intermediate nodes
	for (unsigned short i = 0; i < noOfLeafs; ++i) {
		
		// get the leaf node
		LeafNode &leafNode = this->m_leafNodes[i];
		KeyRange b = { max<Key>(), min<Key>() };
		
		// iterate over the entries in the leaf node to calculate the bounds
		// of the whole leaf node
		for (unsigned short j = 0; j < LEAF_FAN_OUT; ++j) {
			const Entry &entry = leafNode.m_entries[j];
			if (entry.m_bounds[0] < b[0])
				b[0] = entry.m_bounds[0];
			if (entry.m_bounds[1] > b[1])
				b[1] = entry.m_bounds[1];
		}
		
		// at this point, (b[0], b[1]) are the union of the bounds of all
		// entries; these bounds will become an entry in the intermediate node
		
		// calculate the offset of the parent (intermediate) node
        const unsigned short totalOffset = static_cast<unsigned short>(IM_NODES + i);
        const unsigned short parentOffset = static_cast<unsigned short>
                ((totalOffset - 1) / IM_FAN_OUT);
        const unsigned short childIndex = static_cast<unsigned short>
                ((totalOffset - 1) % IM_FAN_OUT);
		assert(parentOffset < IM_NODES - VIRTUAL_IM_NODES);
		
		// make (b[0], b[1]) an entry in the parent (intermediate) node
		IntermediateNode &parentNode = this->m_imNodes[parentOffset];
		parentNode.m_bounds[childIndex][0] = b[0];
		parentNode.m_bounds[childIndex][1] = b[1];
		
	}
	
	// pad unused entries of the lowest-level nodes with illegal bounds
	for (
        unsigned short i = static_cast<unsigned short>(IM_NODES + noOfLeafs - 1);
		i % IM_FAN_OUT;
		++i
	) {
		assert(i / IM_FAN_OUT < IM_NODES - VIRTUAL_IM_NODES);
		IntermediateNode &parentNode = this->m_imNodes[i / IM_FAN_OUT];
		const unsigned short childIndex = i % IM_FAN_OUT;
		parentNode.m_bounds[childIndex][0] = max<Key>();
		parentNode.m_bounds[childIndex][1] = min<Key>();
	}
	
	// the minimal and maximal offset of nodes written to so far
    unsigned short minOffset = static_cast<unsigned short>(IM_NODES);
    unsigned short maxOffset = static_cast<unsigned short>(IM_NODES + noOfLeafs - 1);
	
	// we will now propagate intermediate bounds to higher levels (if any)
	
	// calculate min and max offset of parents
    minOffset = static_cast<unsigned short>((minOffset - 1) / IM_FAN_OUT);
    maxOffset = static_cast<unsigned short>((maxOffset - 1) / IM_FAN_OUT);
	assert(maxOffset < IM_NODES - VIRTUAL_IM_NODES);
	assert(minOffset <= maxOffset);
	
	// we will iterate over all intermediate nodes from right to left,
	// essentially doing iterative dynamic programming to calculate the bounds
	// of every node bottom-up
	
	// one iteration of the outer loop calculates the bounds for a whole
	// level of intermediate nodes
	for (
        unsigned short n = static_cast<unsigned short>(maxOffset - minOffset + 1);
		n > 1;
	) {
		
		// one iteration of the inner loop calculates the bounds for a whole
		// node and puts a corresponding entry into its parent node
		for (unsigned short i = minOffset; i <= maxOffset; ++i) {
			
			IntermediateNode &imNode = this->m_imNodes[i];
			KeyRange b = { max<Key>(), min<Key>() };
			
			// iterate over the entries in the node to calculate the bounds
			// of the whole node 
			for (unsigned short j = 0; j < IM_FAN_OUT; ++j) {
				const KeyRange &imBounds = imNode.m_bounds[j];
				if (imBounds[0] < b[0])
					b[0] = imBounds[0];
				if (imBounds[1] > b[1])
					b[1] = imBounds[1];
			}
			
			// at this point, (b[0], b[1]) are the union of the bounds of all
			// entries; these bounds will become an entry in the parent node
					
			// calculate the offset of the parent node
            const unsigned short parentOffset = static_cast<unsigned short>
                    ((i - 1) / IM_FAN_OUT);
            const unsigned short childIndex = static_cast<unsigned short>
                    ((i - 1) % IM_FAN_OUT);
			assert(parentOffset < IM_NODES - VIRTUAL_IM_NODES);
			IntermediateNode &parentNode = this->m_imNodes[parentOffset];
			
			// make (b[0], b[1]) an entry in the parent node
			parentNode.m_bounds[childIndex][0] = b[0];
			parentNode.m_bounds[childIndex][1] = b[1];
			assert(parentOffset <= minOffset);
			
		}
		
		// pad unused entries in the parent node with illegal bounds
		for (
			unsigned short i = maxOffset;
			i % IM_FAN_OUT;
			++i
		) {
			IntermediateNode &parentNode = this->m_imNodes[i / IM_FAN_OUT];
			assert(i / IM_FAN_OUT < IM_NODES - VIRTUAL_IM_NODES);
			const unsigned short childIndex = i % IM_FAN_OUT;
			parentNode.m_bounds[childIndex][0] = max<Key>();
			parentNode.m_bounds[childIndex][1] = min<Key>();
		}
		
		// remember the range of nodes (offsets) we have written to
        minOffset = static_cast<unsigned short>((minOffset - 1) / IM_FAN_OUT);
        maxOffset = static_cast<unsigned short>((maxOffset - 1) / IM_FAN_OUT);
		
		// calculate number of nodes to process in the next iteration
        n = static_cast<unsigned short>(maxOffset - minOffset + 1);
		
	}
	
	// the entry node is the minimum node written to (= the root)
	this->m_entryNode = minOffset;
	
}

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> inline 
unsigned short OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::find(
	const Key key,
	T * const found
) const {
	return this->find(key, found, this->m_entryNode);
}

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> 
template <typename Visitor>
inline void OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::visit(
	const Key key,
	Visitor &visitor
) const {
	this->visit(key, visitor, this->m_entryNode);
}

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> inline unsigned short
OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::getNoOfElements() 
const throw() {
	return this->m_noOfElements;
}

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> inline unsigned short 
OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::find(
	const Key key,
	T * const found,
	const unsigned short n
) const {
	
	unsigned short noFound = 0;
	
	if (n < IM_NODES) {
	
		// intermediate node
		assert(n < IM_NODES - VIRTUAL_IM_NODES);
		const IntermediateNode &node = this->m_imNodes[n];
		
		// probe each entry
		for (unsigned short i = 0; i < IM_FAN_OUT; ++i) {
            if (key >= node.m_bounds[i][0] && key <= node.m_bounds[i][1]) {
				
				// key is within the child's boundaries, recurse
                noFound = (unsigned short)(noFound + this->find(key, found + noFound,
                    (unsigned short)((n * IM_FAN_OUT) + i + 1)));
				
				// note that we cannot 'break' here since childrens' bounds
				// may overlap
				
			}
		}
		
	} else {
		
		// leaf node
		assert(n >= IM_NODES);
        const unsigned short m = (unsigned short)(n - IM_NODES);
		assert(m < LEAF_NODES);
		const LeafNode &node = this->m_leafNodes[m];
		
		// probe each entry
		for (unsigned short i = 0; i < LEAF_FAN_OUT; ++i) {
            if (key >= node.m_entries[i].m_bounds[0] &&
                    key <= node.m_entries[i].m_bounds[1]) {
				
				// key matches element, put element in answer array
				found[noFound] = node.m_entries[i].m_element;
				++noFound;
				
				// again, we cannot 'break' here since the entries' bounds may
				// overlap
				
			}
		}
		
	}
	
	return noFound;
	
}

template <
	typename T,
	typename Key,
	unsigned short IM_FAN_OUT,
	unsigned short LEAF_FAN_OUT,
	unsigned short SIZE
> 
template <typename Visitor>
inline void OneDimRTree<T, Key, IM_FAN_OUT, LEAF_FAN_OUT, SIZE>::visit(
	const Key key,
	Visitor &visitor,
	const unsigned short n
) const {
	
	if (n < IM_NODES) {
	
		// intermediate node
		assert(n < IM_NODES - VIRTUAL_IM_NODES);
		const IntermediateNode &node = this->m_imNodes[n];
		
		// probe each entry
		for (unsigned short i = 0; i < IM_FAN_OUT; ++i) {
			if (key >= node.m_bounds[i][0] && key <= node.m_bounds[i][1]) {
				
				// key is within the child's boundaries, recurse
				this->visit(key, visitor, (n * IM_FAN_OUT) + i + 1);
				
				// note that we cannot 'break' here since childrens' bounds
				// may overlap
				
			}
		}
		
	} else {
		
		// leaf node
		assert(n >= IM_NODES);
		const unsigned short m = n - IM_NODES;
		assert(m < LEAF_NODES);
		const LeafNode &node = this->m_leafNodes[m];
		
		// probe all entries
		for (unsigned short i = 0; i < LEAF_FAN_OUT; ++i) {
			if (key >= node.m_entries[i].m_bounds[0] &&
					key <= node.m_entries[i].m_bounds[1]) {
				
				// key matches element, call visitor
				visitor(node.m_entries[i].m_element);
				
				// again, we cannot 'break' here since the entries' bounds may
				// overlap
				
			}
		}
		
	}
	
}

}
