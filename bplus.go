package bplus

type fillState int

const (
	nodeEmpty         fillState = iota // The node is empty
	nodePartiallyFull                  // The node is partially full
	nodeFull                           // The node is completely full
)

type (
	Key   []byte // A byte-slice representing a key in a B+ tree
	Value []byte // A byte-slice representing a leaf-node key's value in a B+ tree
)

// BPlusTree
type BPlusTree struct {
	order int  // Max number of keys per node
	root  node // The root node (either leaf or internal)
}

// NewBPlusTree creates a new B+ tree with the specified order.
func NewBPlusTree(order int) *BPlusTree {
	if order <= 0 {
		panic("tree order must be >= 1")
	}
	return &BPlusTree{
		order: order,
		root:  newLeafNode(order),
	}
}

// Order returns the B+ tree's order (the max number of keys
// allowes per node).
func (t *BPlusTree) Order() int {
	return t.order
}

// GetKeys returns a slice of all keys in the tree
func (t *BPlusTree) GetKeys() ([]Key, error) {
	return t.root.GetKeys()
}

// GetValues returns a slice of all values in the tree
func (t *BPlusTree) GetValues() ([]Value, error) {
	return t.root.GetValues()
}

// Search returns the first value with the matching
// key in the tree
func (t *BPlusTree) Search(k Key) (Value, error) {
	return t.root.Search(k)
}

// SearchRange returns all values with a key
func (t *BPlusTree) SearchRange(k1, k2 Key) ([]Value, error) {
	return t.root.SearchRange(k1, k2)
}

// Insert adds a new value to the tree, at the given key
func (t *BPlusTree) Insert(k Key, v Value) error {
	return t.root.Insert(k, v)
}

// Update sets the value for all matching keys in the tree
func (t *BPlusTree) Update(k Key, v Value) error {
	return t.root.Update(k, v)
}

// Delete removes all key-value pairs from the tree with
// a matching key
func (t *BPlusTree) Delete(k Key) error {
	return t.root.Delete(k)
}

// node represents a node (either internal or leaf)
// in the B+ tree.
type node interface {
	// GetKeys returns a slice of all keys in the tree
	GetKeys() ([]Key, error)

	// GetValues returns a slice of all values in the tree
	GetValues() ([]Value, error)

	// Search returns the first value with the matching
	// key in the tree
	Search(Key) (Value, error)

	// SearchRange returns all values with a key
	SearchRange(Key, Key) ([]Value, error)

	// Insert adds a new value to the tree, at the given key
	Insert(Key, Value) error

	// Update sets the value for all matching keys in the tree
	Update(Key, Value) error

	// Delete removes all key-value pairs from the tree with
	// a matching key
	Delete(Key) error

	// getFillState checks if the node's key slice is empty,
	// partially full, or completely full.
	getFillState() fillState
}

// internalNode stores pointers to other nodes in a B+ tree.
type internalNode struct {
	// Max number of keys in the node
	order int

	// Slice of keys in the node. The values less than the
	// i-th key will be in the i-th pointer. Values greater
	// than the i-th key (but less than the i+1-th key) will
	// be in the i+1th pointer.
	keys []Key

	// Pointers to child nodes. Pointer i points to the
	// node to the left of Key i.
	//
	//   len(n.pointers) == len(n.keys) + 1
	//
	// There will be one more pointer than key as, for
	// n keys, the n+1th pointer will point to the node
	// to the right of the nth key.
	//
	pointers []node
}

// newInternalNode creates a new internalNode for a B+ tree
// with the specified order. The node can hold a maximum of
// `order` nodes.
//
// For node n:
//
//   len(n.keys) == n.order
//
// and:
//
//   len(n.pointers) == n.order + 1
//
func newInternalNode(order int) *internalNode {
	return &internalNode{
		order:    order,
		keys:     make([]Key, order),
		pointers: make([]node, order+1),
	}
}

func (n *internalNode) getFillState() fillState {
	for i, k := range n.keys {
		if k != nil {
			continue
		}
		if i == 0 {
			return nodeEmpty
		}
		return nodePartiallyFull
	}
	return nodeFull
}

func (n *internalNode) GetKeys() ([]Key, error) {
	var keys []Key

	// For each child node...
	for _, p := range n.pointers {
		// Is it empty?
		if p == nil {
			break
		}

		// Get child node's keys...
		k, err := p.GetKeys()
		if err != nil {
			return nil, err
		}

		// Add them to the slice...
		keys = append(keys, k...)
	}
	return keys, nil
}

func (n *internalNode) GetValues() ([]Value, error) {
	var vals []Value

	// For each child node...
	for _, p := range n.pointers {
		// Is it empty?
		if p == nil {
			break
		}

		// Get child node's values...
		v, err := p.GetValues()
		if err != nil {
			return nil, err
		}

		// Add them to the slice...
		vals = append(vals, v...)
	}
	return vals, nil
}

func (n *internalNode) Search(k Key) (Value, error) {
	return nil, nil
}

func (n *internalNode) SearchRange(k1, k2 Key) ([]Value, error) {
	return nil, nil
}

func (n *internalNode) Insert(k Key, v Value) error {
	return nil
}

func (n *internalNode) Update(k Key, v Value) error {
	return nil
}

func (n *internalNode) Delete(k Key) error {
	return nil
}

// leafNode stores the data for a B+ tree leaf node.
type leafNode struct {
	order    int     // Max number of keys in the node
	keys     []Key   // Slice of keys contained in the node
	pointers []Value // "Pointers" to the data mapped to each corresponding key
	next     node    // A pointer to the next leaf node (if any)
}

// newLeafNode creates a new leaf node with the specified order.
func newLeafNode(order int) *leafNode {
	return &leafNode{
		order:    order,
		keys:     make([]Key, order),
		pointers: make([]Value, order),
	}
}

func (n *leafNode) getFillState() fillState {
	for i, k := range n.keys {
		if k != nil {
			continue
		}
		if i == 0 {
			return nodeEmpty
		}
		return nodePartiallyFull
	}
	return nodeFull
}

func (n *leafNode) GetKeys() ([]Key, error) {
	var keys []Key
	for _, k := range n.keys {
		if k == nil {
			break
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func (n *leafNode) GetValues() ([]Value, error) {
	var vals []Value
	for _, v := range n.pointers {
		if v == nil {
			break
		}
		vals = append(vals, v)
	}
	return vals, nil
}

func (n *leafNode) Search(k Key) (Value, error) {
	return nil, nil
}

func (n *leafNode) SearchRange(k1, k2 Key) ([]Value, error) {
	return nil, nil
}

func (n *leafNode) Insert(k Key, v Value) error {
	return nil
}

func (n *leafNode) Update(k Key, v Value) error {
	return nil
}

func (n *leafNode) Delete(k Key) error {
	return nil
}
