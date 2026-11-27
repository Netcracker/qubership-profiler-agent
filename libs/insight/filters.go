package insight

import (
	"slices"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Filters are the conditions of an operation (02 §2.3.3) in their text form,
// plus the Service shorthand.
type Filters struct {
	// Conditions are sent to the query service unchanged. The client checks
	// their syntax only, so a field or operator the server gains later works
	// without a client change.
	Conditions []string
	// Service is "<namespace>/<service>", shorthand for two conditions.
	Service string
}

// conditions returns every condition as text, the Service shorthand expanded.
func (f Filters) conditions() ([]string, error) {
	out := slices.Clone(f.Conditions)
	if f.Service != "" {
		ns, svc, ok := strings.Cut(f.Service, "/")
		if !ok || ns == "" || svc == "" || strings.Contains(svc, "/") {
			return nil, errors.Errorf("service %q is not <namespace>/<service>", f.Service)
		}
		out = append(out, "namespace="+ns, "service="+svc)
	}
	for _, text := range out {
		if _, err := model.ParseCondition(text); err != nil {
			return nil, errors.Wrapf(err, "filter %q", text)
		}
	}
	return out, nil
}

// namespaces returns the namespaces the conditions select with = or in, for
// the Authorizer. Empty means the query names no namespace.
func namespaces(conditions []string) []string {
	var out []string
	for _, text := range conditions {
		c, err := model.ParseCondition(text)
		if err != nil || c.Field != "namespace" || (c.Op != model.OpEq && c.Op != model.OpIn) {
			continue
		}
		for _, v := range c.Values {
			if !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
	}
	return out
}
