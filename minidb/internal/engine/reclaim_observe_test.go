package engine

import "slices"

type ReclaimObservation struct {
	Sequence  uint64
	Snapshots []uint64
	Waiters   int
	Tables    map[string]VersionObservation
}

type VersionObservation struct {
	Current, Private, Required, Deferred, Eligible int
	Heap, Primary, Secondary                       int
}

// ObserveReclamation is test-only. The watermark is sampled together with
// the pins. Table counts can advance between tables during live writes.
func (db *DB) ObserveReclamation(names ...string) ReclaimObservation {
	db.snapshots.mu.Lock()
	sequence := db.published.Load()
	obs := ReclaimObservation{Sequence: sequence, Tables: make(map[string]VersionObservation)}
	for pin := range db.snapshots.active {
		obs.Snapshots = append(obs.Snapshots, pin.sequence)
	}
	db.snapshots.mu.Unlock()
	db.locks.mu.Lock()
	obs.Waiters = len(db.locks.waiting)
	db.locks.mu.Unlock()
	slices.Sort(obs.Snapshots)
	oldest := sequence
	if len(obs.Snapshots) != 0 {
		oldest = obs.Snapshots[0]
	}
	for _, name := range names {
		tbl, ok := db.table(name, sequence)
		if !ok {
			continue
		}
		tbl.mu.RLock()
		var counts VersionObservation
		for _, page := range tbl.pages {
			for _, row := range page {
				counts.Heap++
				created := row.created.sequence.Load()
				var ended uint64
				if row.ended != nil {
					ended = row.ended.sequence.Load()
				}
				switch {
				case created == abortedSequence:
					counts.Eligible++
				case created == 0 || created > sequence:
					counts.Private++
				case ended == 0 || ended == abortedSequence || ended > sequence:
					counts.Current++
				case ended <= oldest:
					counts.Eligible++
				default:
					if slices.ContainsFunc(obs.Snapshots, func(pin uint64) bool {
						return created <= pin && pin < ended
					}) {
						counts.Required++
					} else {
						counts.Deferred++
					}
				}
			}
		}
		for _, idx := range tbl.indexes {
			count := countCandidates(&idx, idx.root)
			if idx.def.Primary {
				counts.Primary += count
			} else {
				counts.Secondary += count
			}
		}
		tbl.mu.RUnlock()
		obs.Tables[name] = counts
	}
	return obs
}

func countCandidates(idx *index, page *indexPage) int {
	var result int
	for _, child := range page.children {
		result += countCandidates(idx, child)
	}
	for _, row := range page.rows {
		for row != nil {
			result++
			row = row.indexLinks[idx.slot].next
		}
	}
	return result
}
