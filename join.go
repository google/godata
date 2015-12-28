package godata

import (
	"fmt"

	"github.com/google/godata/row"
)

// JoinResult represents the result of a join operation.
type JoinResult struct {
	// Left contains the contents in the left side of the join, or nil if the
	// left frame did not contain a row for the key. Left must contain the same
	// index as Right.
	Left interface{}

	// Right contains the contents in the right side of the join, or nil if the
	// right frame did not contain a row for the key. Left must contain the same
	// index as Right.
	Right interface{}
}

// String returns the string representation of this JoinResult.
func (j JoinResult) String() string {
	if j.Left != nil && j.Right != nil {
		return fmt.Sprintf("JoinResult{Left: %v, Right: %v}", j.Left, j.Right)
	}
	if j.Left == nil {
		return fmt.Sprintf("JoinResult{Right: %v}", j.Right)
	}
	if j.Right == nil {
		return fmt.Sprintf("JoinResult{Left: %v}", j.Left)
	}
	return "JoinResult{}"
}

// JoinResultIndexer indexes a JoinResult by delegating to the given RowIndexer.
type JoinResultIndexer struct {
	// RowIndexer is the indexer to use for the RowData in either Left or Right.
	RowIndexer row.Indexer
}

// Index returns the index of the contents of a JoinResult. At least one of
// Left and Right must be non-nil. If both Left and Right are non-nil, then
// they must have the same index value.
func (j JoinResultIndexer) Index(data row.Data) (row.Index, error) {
	projection := make(map[string]interface{})

	for key, val := range data {
		r, ok := val.(*JoinResult)
		if !ok {
			projection[key] = val
			continue
		}
		if r.Left != nil {
			projection[key] = r.Left
		} else if r.Right != nil {
			projection[key] = r.Right
		} else {
			projection[key] = nil
		}
	}

	return j.RowIndexer.Index(projection)
}
