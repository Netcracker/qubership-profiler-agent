package inventory

import (
	"sort"
)

type Services struct { // not thread-safe!
	set map[string]bool
}

func NewServices() Services {
	return Services{set: map[string]bool{}}
}

func (s *Services) AddMap(serviceSet map[string]any) {
	for service := range serviceSet {
		s.set[service] = true
	}
}

func (s *Services) AddList(serviceList []string) {
	for _, service := range serviceList {
		s.set[service] = true
	}
}

func (s *Services) Size() int {
	return len(s.set)
}

func (s *Services) List() interface{} {
	list := make([]string, 0, len(s.set))
	for service := range s.set {
		list = append(list, service)
	}
	sort.Strings(list)
	return list
}
