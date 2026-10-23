package mysqlstore

import "sort"

// sortedUnique is the single lock order for account rows. Callers lock
// accounts one by one in this order so A→B and B→A cannot deadlock.
// UUID strings are compared lexicographically; every code path must use
// this function rather than sorting on its own.
func sortedUnique(ids []string) []string {
	cp := append([]string(nil), ids...)
	sort.Strings(cp)
	out := cp[:0]
	for _, id := range cp {
		if id == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}
