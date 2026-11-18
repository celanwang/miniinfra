package engine

// IndexCandidates counts the physical versions linked to one equality key.
func (db *DB) IndexCandidates(table, indexName string, values []Value) (int, bool) {
	tbl, ok := db.table(table, db.published.Load())
	if !ok {
		return 0, false
	}
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	for i := range tbl.indexes {
		idx := &tbl.indexes[i]
		if idx.def.Name == indexName {
			count := 0
			for range idx.lookup(lookupKey(values)) {
				count++
			}
			return count, true
		}
	}
	return 0, false
}
