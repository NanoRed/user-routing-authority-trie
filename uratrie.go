package uratrie

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

type Node struct {
	// segment => *regexp.Regexp
	Regexps map[string]*regexp.Regexp
	// segment => *Node
	Childrens map[string]*Node
	// userIDs
	Container *roaring.Bitmap
	// because it is a different branch, it can only be all single or all multiple.
	// segment index => wildcard key => path key
	Wildcards map[int]map[string]string
	// whether leaf node
	Leaf bool
}

func (n *Node) ipop(
	path *[]byte,
) (
	segment string,
	wildcard string,
	fuzzy bool,
) {
	if len(*path) == 0 {
		return
	}
	s, c, bs, be := 0, 0, -1, -1
	b := bufPool.New().(*bytes.Buffer)
	wb := bufPool.New().(*bytes.Buffer)
	for i := 0; i < len(*path); i++ {
		switch (*path)[i] {
		case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
			(*path)[i] += 32 // lower case
		case '{':
			bs = i
		case '}':
			be = i
		case '/':
			if b.Len() == 0 && s == i {
				s = i + 1
				continue
			}
			b.Write((*path)[s:i])
			segment = b.String()
			b.Reset()
			if wb.Len() > 0 {
				wildcard = wb.String()
				if c == 1 {
					wildcard = wildcard[1:]
				}
				wb.Reset()
			}
			bufPool.Put(b)
			bufPool.Put(wb)
			*path = (*path)[i+1:]
			return
		}
		if bs >= s && be > bs {
			wb.WriteByte(';')
			wb.Write((*path)[bs+1 : be])
			b.Write((*path)[s:bs])
			b.WriteByte('*')
			c++
			s = be + 1
			bs, be = -1, -1
			fuzzy = true
		}
	}
	b.Write((*path)[s:])
	segment = b.String()
	b.Reset()
	if wb.Len() > 0 {
		wildcard = wb.String()
		if c == 1 {
			wildcard = wildcard[1:]
		}
		wb.Reset()
	}
	bufPool.Put(b)
	bufPool.Put(wb)
	*path = (*path)[len(*path):]
	return
}

func (n *Node) Insert(key string, path []byte, userIDs []uint32) (err error) {
	return n.insert(key, path, userIDs, nil, 0)
}

func (n *Node) insert(
	key string,
	path []byte,
	userIDs []uint32,
	// segment index => [0 -> wildcard 1 -> key]
	wildcards map[int][2]string,
	cur int,
) (
	err error,
) {
RESTART:
	if len(path) == 0 {
		if len(userIDs) > 0 {
			sort.Slice(userIDs, func(i, j int) bool {
				return userIDs[i] < userIDs[j]
			})
			if n.Leaf {
				if n.Container != nil {
					if userIDs[0] == 0 {
						n.Container = nil
					} else {
						n.Container.AddMany(userIDs)
					}
				}
			} else {
				n.Leaf = true
				if userIDs[0] != 0 {
					n.Container = roaring.New()
					n.Container.AddMany(userIDs)
				}
			}
			if len(wildcards) > 0 {
				if n.Wildcards == nil {
					n.Wildcards = make(map[int]map[string]string)
				}
				for i, val := range wildcards {
					if _, ok := n.Wildcards[i]; !ok {
						n.Wildcards[i] = make(map[string]string)
					}
					n.Wildcards[i][val[0]] = val[1]
				}
			}
		}
		return
	}
	segment, wildcard, fuzzy := n.ipop(&path)
	if len(segment) == 0 {
		goto RESTART
	} else if len(wildcard) > 0 {
		if wildcards == nil {
			wildcards = make(map[int][2]string)
		}
		wildcards[cur] = [2]string{wildcard, key}
	}
	if fuzzy && (len(segment) > 1 || segment[0] != '*') {
		b := bufPool.New().(*bytes.Buffer)
		b.WriteByte('^')
		b.WriteString(strings.ReplaceAll(segment, "*", "(.+?)"))
		b.WriteByte('$')
		segment = b.String()
		if n.Regexps == nil {
			n.Regexps = make(map[string]*regexp.Regexp)
		}
		if _, ok := n.Regexps[segment]; !ok {
			var reg *regexp.Regexp
			if reg, err = regexp.Compile(segment); err != nil {
				return
			}
			n.Regexps[segment] = reg
		}
	}
	if n.Childrens == nil {
		n.Childrens = make(map[string]*Node)
	}
	if child, x := n.Childrens[segment]; x {
		return child.insert(key, path, userIDs, wildcards, cur+1)
	}
	n.Childrens[segment] = &Node{}
	return n.Childrens[segment].insert(key, path, userIDs, wildcards, cur+1)
}

func (n *Node) mpop(path *[]byte) (segment string) {
	if len(*path) == 0 {
		return
	}
	s := 0
	for i := 0; i < len(*path); i++ {
		switch (*path)[i] {
		case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
			(*path)[i] += 32 // lower case
		case '/':
			if s == i {
				s = i + 1
				continue
			}
			segment = string((*path)[s:i])
			*path = (*path)[i+1:]
			return
		}
	}
	segment = string((*path)[s:])
	*path = (*path)[len(*path):]
	return
}

func (n *Node) Match(path []byte, userID uint32) (has, ok bool, params map[string]string) {
	var wildcardElements []map[string][2]string
	has, ok, wildcardElements = n.match(path, userID, 0, make(map[int]string))
	if len(wildcardElements) > 0 {
		params = make(map[string]string)
		for _, val := range wildcardElements {
			for wildcard, v := range val {
				if wildcard[0] == ';' {
					keys := strings.Split(wildcard, ";")
					vals := strings.Split(v[0], ";")
					for i := 1; i < len(keys); i++ {
						params[keys[i]] = vals[i]
					}
				} else {
					params[wildcard] = v[0]
				}
			}
		}
	}
	return
}

func (n *Node) match(
	path []byte,
	userID uint32,
	cur int,
	segmentWildcardElements map[int]string,
) (
	has, ok bool,
	// wildcard key => [0 -> element 1 -> key]
	wildcardElements []map[string][2]string,
) {
	if len(path) == 0 {
		if !n.Leaf {
			return
		}
		has = true
		if len(n.Wildcards) > 0 {
			for cur, val := range n.Wildcards {
				tmp := make(map[string][2]string)
				for wildcard, key := range val {
					tmp[wildcard] = [2]string{segmentWildcardElements[cur], key}
				}
				wildcardElements = append(wildcardElements, tmp)
			}
		}
		if userID == 0 {
			ok = n.Container == nil
		} else {
			if n.Container != nil {
				ok = n.Container.Contains(userID)
			} else {
				ok = true
			}
		}
		return
	}
	segment := n.mpop(&path)

	//     I have done an optimization experiment that using goroutine, and the results
	// show that although the overall average time-consuming is much more stable, the
	// overall performance has declined.
	//     Maybe we can try to use the relevant implementation of goroutine pool for
	// further optimization.
	//
	// Update:
	//     Already tried, goroutine pool solution leads to worse performance. lib tested:
	// https://github.com/panjf2000/ants

	// type1: regexp
	for pattern, reg := range n.Regexps {
		if matches := reg.FindStringSubmatch(segment); matches != nil {
			matches[0] = ""
			segmentWildcardElements[cur] = strings.Join(matches, ";")
			if _has, _ok, _wildcardElements := n.Childrens[pattern].match(path, userID, cur+1, segmentWildcardElements); _ok {
				has, ok, wildcardElements = _has, _ok, _wildcardElements
				return
			} else if _has {
				has, wildcardElements = _has, _wildcardElements
			}
		}
	}
	// type2: key
	if child, x := n.Childrens[segment]; x {
		if _has, _ok, _wildcardElements := child.match(path, userID, cur+1, segmentWildcardElements); _ok {
			has, ok, wildcardElements = _has, _ok, _wildcardElements
			return
		} else if _has {
			has, wildcardElements = _has, _wildcardElements
		}
	}
	// type3: all
	if child, x := n.Childrens["*"]; x {
		segmentWildcardElements[cur] = segment
		if _has, _ok, _wildcardElements := child.match(path, userID, cur+1, segmentWildcardElements); _ok {
			has, ok, wildcardElements = _has, _ok, _wildcardElements
			return
		} else if _has {
			has, wildcardElements = _has, _wildcardElements
		}
	}
	return
}

func (n *Node) Dump(text *string, part ...string) {
	p := ""
	if len(part) > 0 {
		p = part[0]
	}
	if n.Leaf {
		if n.Container == nil {
			*text += fmt.Sprintf("%s %v -\n", p, n.Wildcards)
		} else {
			*text += fmt.Sprintf("%s %v %v\n", p, n.Wildcards, n.Container.ToArray())
		}
	}
	for s, node := range n.Childrens {
		node.Dump(text, p+"/"+s)
	}
}
