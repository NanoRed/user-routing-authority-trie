package uratrie

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type Node struct {
	Regexps   map[string]*regexp.Regexp
	Childrens map[string]*Node // segment => node
	Container *roaring64.Bitmap
	Labels    map[string]string
	End       bool
}

func (n *Node) ipop(path *[]byte) (seg, label string, w bool) {
	if len(*path) == 0 {
		return
	}
	s := 0
	xlab := false
	lbuf := &bytes.Buffer{}
	for i := 0; i < len(*path); i++ {
		switch (*path)[i] {
		case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
			(*path)[i] += 32 // lower case
		case '/':
			seg = string((*path)[s:i])
			s = i + 1
			if len(seg) == 0 {
				continue
			} else {
				if !xlab && lbuf.Len() > 0 {
					label = lbuf.String()[1:]
				}
				*path = (*path)[s:]
				return
			}
		case '[':
			xlab = true
		case ']':
			xlab = false
		case '*':
			if !xlab {
				w = true
			}
		}
		if xlab {
			lbuf.WriteByte((*path)[i])
		}
	}
	seg = string((*path)[s:])
	if !xlab && lbuf.Len() > 0 {
		label = lbuf.String()[1:]
	}
	*path = (*path)[len(*path):]
	return
}

func (n *Node) Insert(path []byte, value []uint64, labels map[string]string) (err error) {
RESTART:
	if len(path) == 0 {
		if len(value) > 0 {
			if n.Container == nil {
				n.Container = roaring64.New()
			}
			n.Container.AddMany(value)
		}
		n.Labels = labels
		n.End = true
		return
	}
	seg, label, wildcard := n.ipop(&path)
	if len(label) > 0 {
		seg = seg[len(label)+2:]
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[label] = seg
		if len(seg) == 0 {
			goto RESTART
		}
	}
	if wildcard && (len(seg) > 1 || seg[0] != '*') {
		buf := &bytes.Buffer{}
		buf.WriteByte('^')
		buf.WriteString(strings.ReplaceAll(seg, "*", "(.+?)"))
		buf.WriteByte('$')
		seg = buf.String()
		if n.Regexps == nil {
			n.Regexps = make(map[string]*regexp.Regexp)
		}
		if _, ok := n.Regexps[seg]; !ok {
			var reg *regexp.Regexp
			if reg, err = regexp.Compile(seg); err != nil {
				return
			}
			n.Regexps[seg] = reg
		}
	}
	if n.Childrens == nil {
		n.Childrens = make(map[string]*Node)
	}
	if child, x := n.Childrens[seg]; x {
		return child.Insert(path, value, labels)
	}
	n.Childrens[seg] = &Node{}
	return n.Childrens[seg].Insert(path, value, labels)
}

func (n *Node) mpop(path *[]byte) (seg string) {
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
			seg = string((*path)[s:i])
			s = i + 1
			if seg == "" {
				continue
			} else {
				*path = (*path)[s:]
				return
			}
		}
	}
	seg = string((*path)[s:])
	*path = (*path)[len(*path):]
	return
}

func (n *Node) Match(path []byte, value uint64) (has, ok bool, labels map[string]string) {
	if len(path) == 0 {
		if !n.End {
			return
		}
		has = true
		labels = n.Labels
		if value == 0 {
			ok = n.Container == nil
		} else if n.Container != nil {
			ok = n.Container.Contains(value)
		}
		return
	}
	seg := n.mpop(&path)

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
		if reg.MatchString(seg) {
			if _has, _ok, _labels := n.Childrens[pattern].Match(path, value); _ok {
				has, ok, labels = _has, _ok, _labels
				return
			} else if _has {
				has, labels = _has, _labels
			}
		}
	}
	// type2: key
	if child, x := n.Childrens[seg]; x {
		if _has, _ok, _labels := child.Match(path, value); _ok {
			has, ok, labels = _has, _ok, _labels
			return
		} else if _has {
			has, labels = _has, _labels
		}
	}
	// type3: wildcard
	if child, x := n.Childrens["*"]; x {
		if _has, _ok, _labels := child.Match(path, value); _ok {
			has, ok, labels = _has, _ok, _labels
			return
		} else if _has {
			has, labels = _has, _labels
		}
	}
	return
}

func (n *Node) Dump(text *string, part ...string) {
	p := ""
	if len(part) > 0 {
		p = part[0]
	}
	if n.End {
		if n.Container == nil {
			*text += fmt.Sprintf("%s %v -\n", p, n.Labels)
		} else {
			*text += fmt.Sprintf("%s %v %v\n", p, n.Labels, n.Container.ToArray())
		}
	}
	for s, node := range n.Childrens {
		node.Dump(text, p+"/"+s)
	}
}
