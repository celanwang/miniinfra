package engine

import (
	"slices"
	"sort"
)

const indexPageKeys = 32

// The leaves own index entries. Internal pages route lookups using the
// first key of each right subtree.
type indexPage struct {
	keys     []string
	rows     [][]*version
	children []*indexPage
}

type index struct {
	def  Index
	root *indexPage
}

func (idx *index) lookup(key string) []*version {
	page := idx.root
	for len(page.children) != 0 {
		page = page.children[sort.Search(len(page.keys), func(i int) bool { return key < page.keys[i] })]
	}
	if i, found := slices.BinarySearch(page.keys, key); found {
		return page.rows[i]
	}
	return nil
}

func (idx *index) add(key string, v *version) {
	if split, right := idx.root.insert(key, v); right != nil {
		idx.root = &indexPage{keys: []string{split}, children: []*indexPage{idx.root, right}}
	}
}

func (page *indexPage) insert(key string, v *version) (string, *indexPage) {
	if len(page.children) == 0 {
		at, found := slices.BinarySearch(page.keys, key)
		if found {
			page.rows[at] = append(page.rows[at], v)
			return "", nil
		}
		page.keys = slices.Insert(page.keys, at, key)
		page.rows = slices.Insert(page.rows, at, []*version{v})
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

	at := sort.Search(len(page.keys), func(i int) bool { return key < page.keys[i] })
	split, right := page.children[at].insert(key, v)
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
