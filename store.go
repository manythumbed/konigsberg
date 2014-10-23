package storage

type NodeStore interface {
	Fetch(index Index) (*Node, bool)
	Store(index Index, node Node) bool
}

type RelationshipStore interface {
	Fetch(index Index) (*Relationship, bool)
	Store(index Index, relationship Relationship) bool
}
