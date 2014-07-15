package helper

import(
    "sort"
)

//对map排序

type MapSorter []MapItem
type MapItem struct {
    Key string
    Val float64
}

func (ms MapSorter) Len() int {
    return len(ms)
}

func (ms MapSorter) Less(i, j int) bool {
    return ms[i].Key < ms[j].Key
}

func (ms MapSorter) Swap(i, j int) {
    ms[i], ms[j] = ms[j], ms[i]
}

func NewMapSorter(m map[string]float64) MapSorter {
    ms := make(MapSorter, 0, len(m))

    for k,v := range m {
        ms = append(ms, MapItem{k, v})
    }

    sort.Sort(ms)
    return ms
}
