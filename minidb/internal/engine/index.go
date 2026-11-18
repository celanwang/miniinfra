package engine

import (
	"iter"
	"slices"
	"sort"
)

const indexPageKeys = 32

type indexLink struct {
	previous, next *version
}

// A leaf has one list of candidates per key. Versions carry the links so
// removing one candidate does not scan or copy a hot key's history.
type indexPage struct {
	keys     []string
	rows     []*version
	children []*indexPage
}

type index struct {
	def  Index
	root *indexPage
	slot int
}

func (idx *index) lookup(key string) iter.Seq[*version] {
	return func(yield func(*version) bool) {
		page := idx.root.leaf(key)
		at, found := slices.BinarySearch(page.keys, key)
		if !found {
			return
		}
		for row := page.rows[at]; row != nil; row = row.indexLinks[idx.slot].next {
			if !yield(row) {
				return
			}
		}
	}
}

func (page *indexPage) child(key string) int {
	return sort.Search(len(page.keys), func(i int) bool { return key < page.keys[i] })
}

func (page *indexPage) leaf(key string) *indexPage {
	for len(page.children) != 0 {
		page = page.children[page.child(key)]
	}
	return page
}

func (idx *index) add(key string, v *version) {
	if split, right := idx.root.insert(key, v, idx.slot); right != nil {
		idx.root = &indexPage{keys: []string{split}, children: []*indexPage{idx.root, right}}
	}
}

func (page *indexPage) insert(key string, v *version, slot int) (string, *indexPage) {
	if len(page.children) == 0 {
		at, found := slices.BinarySearch(page.keys, key)
		if found {
			head := page.rows[at]
			head.indexLinks[slot].previous = v
			v.indexLinks[slot].next = head
			page.rows[at] = v
			return "", nil
		}
		page.keys = slices.Insert(page.keys, at, key)
		page.rows = slices.Insert(page.rows, at, v)
		if len(page.keys) <= indexPageKeys {
			return "", nil
		}
		mid := len(page.keys) / 2
		right := &indexPage{keys: slices.Clone(page.keys[mid:]), rows: slices.Clone(page.rows[mid:])}
		clear(page.keys[mid:])
		clear(page.rows[mid:])
		page.keys, page.rows = page.keys[:mid], page.rows[:mid]
		return right.keys[0], right
	}

	at := page.child(key)
	split, right := page.children[at].insert(key, v, slot)
	if right == nil {
		return "", nil
	}
	page.keys = slices.Insert(page.keys, at, split)
	page.children = slices.Insert(page.children, at+1, right)
	if len(page.keys) <= indexPageKeys {
		return "", nil
	}
	mid := len(page.keys) / 2
	pivot := page.keys[mid]
	sibling := &indexPage{keys: slices.Clone(page.keys[mid+1:]), children: slices.Clone(page.children[mid+1:])}
	clear(page.keys[mid:])
	clear(page.children[mid+1:])
	page.keys, page.children = page.keys[:mid], page.children[:mid+1]
	return pivot, sibling
}

func (idx *index) remove(row *version) {
	link := &row.indexLinks[idx.slot]
	if link.previous != nil {
		link.previous.indexLinks[idx.slot].next = link.next
	} else {
		// Only removing the head changes the leaf entry for this key.
		key, _ := key(row.values, idx.def.Columns)
		if link.next != nil {
			page := idx.root.leaf(key)
			at, _ := slices.BinarySearch(page.keys, key)
			page.rows[at] = link.next
		} else {
			idx.root.removeKey(key)
			for len(idx.root.children) == 1 {
				idx.root = idx.root.children[0]
			}
		}
	}
	if link.next != nil {
		link.next.indexLinks[idx.slot].previous = link.previous
	}
	*link = indexLink{}
}

// Drop empty children. Separators between surviving children remain valid
// routing bounds even if the minimum key in a leaf increases.
func (page *indexPage) removeKey(key string) bool {
	if len(page.children) == 0 {
		at, _ := slices.BinarySearch(page.keys, key)
		page.keys = slices.Delete(page.keys, at, at+1)
		page.rows = slices.Delete(page.rows, at, at+1)
		return len(page.keys) == 0
	}
	at := page.child(key)
	if !page.children[at].removeKey(key) {
		return false
	}
	page.children = slices.Delete(page.children, at, at+1)
	if len(page.keys) != 0 {
		boundary := max(0, at-1)
		page.keys = slices.Delete(page.keys, boundary, boundary+1)
	}
	return len(page.children) == 0
}
