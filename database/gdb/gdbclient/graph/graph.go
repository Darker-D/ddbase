package graph

import (
	"fmt"
	"strings"
)

type Element interface {
	// Gets the unique identifier for the graph Element
	Id() string

	// Gets the label for the graph Element which helps categorize it
	Label() string

	// Get a Property for the Element given its key
	Property(key string) Property

	// Get slice of Property where the keys is meant to be a filter on the available Keys.
	// If no keys are provide then return all the properties
	Properties(keys ...string) []Property

	// Get the value of a Property given it's key.
	Value(key string) interface{}

	// Get slice of the values of properties
	// If no keys are provide then return all the values
	Values(keys ...string) []interface{}

	// Get the keys of the properties associated with this element
	Keys() []string
}

type Property interface {
	// The key of the property
	PKey() string

	// The value of the property
	PValue() interface{}

	// Get the element that this property is associated with
	PElement() Element

	fmt.Stringer
}

// Vertex <--  Element
type Vertex interface {
	Element

	// Gets slice of incident edges
	Edges(out bool, label ...string) []Edge

	// Gets slice of adjacent vertices
	Vertices(out bool, label ...string) []Vertex

	// Get the VertexProperty for the provided key
	VProperty(key string) VertexProperty

	// Get slice of properties with provide keys
	VProperties(keys ...string) []VertexProperty

	fmt.Stringer
}

// VertexProperty <-- Property
// VertexProperty <-- Element
type VertexProperty interface {
	Element
	Property

	// override ??
	VElement() Vertex

	// override ??
	VLabel() string
}

// Edge <-- Element
type Edge interface {
	Element

	// Get the outgoing vertex of this edge
	InVertex() Vertex

	// Get the incoming vertex of the edge
	OutVertex() Vertex

	fmt.Stringer
}

// Path for GDB
type Path interface {
	Size() int

	Objects() []interface{}

	Labels() [][]string

	fmt.Stringer
}

type BulkSet struct {
	values map[interface{}]int64
}

func NewBulkSet() *BulkSet {
	return &BulkSet{values: make(map[interface{}]int64, 0)}
}

func (b *BulkSet) Add(s interface{}, bulk int64) {
	b.values[s] = bulk
}

func (b *BulkSet) UniqueSize() int {
	return len(b.values)
}

func (b *BulkSet) Size() int {
	var count int64 = 0
	for _, v := range b.values {
		count += v
	}
	return int(count)
}

func (b *BulkSet) IsEmpty() bool {
	return len(b.values) == 0
}

func (b *BulkSet) AsBulk() map[interface{}]int64 {
	return b.values
}

func (b *BulkSet) String() string {
	var output []string
	for k, v := range b.values {
		output = append(output, fmt.Sprintf("{%v : %d}", k, v))
	}
	return fmt.Sprintf("{%s}", strings.Join(output, ","))
}
