package manager

import (
	"sync"
)

// MapperManager is a manager for map from mapper to node
type MapperManager struct {
	// Mapper2NodeMap, key is mapper.Name, value is node.Name
	// TODO mapper.Name在多个节点上可能重复
	Mapper2NodeMap sync.Map

	// NodeMapperList stores the mapper list deployed on the corresponding node, key is node.Name, value is *[]v1beta1.MapperInfo{}
	NodeMapperList sync.Map
}

// NewMapperManager is function to return new MapperManager
func NewMapperManager() *MapperManager {
	return &MapperManager{}
}
