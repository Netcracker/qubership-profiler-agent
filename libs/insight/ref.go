package insight

import (
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// CallRef identifies one call together with the cold-tier hints that /tree
// needs to find it (02 §2.2), so an agent never assembles the hints itself.
// Its text form is "<pk>@<ts_ms>@<retention_class>". The client builds it;
// a server-issued call_ref is deferred.
type CallRef struct {
	PK             model.PK
	TsMs           int64
	RetentionClass string
}

// String returns the text form of the reference.
func (r CallRef) String() string {
	return r.PK.PathString() + "@" + strconv.FormatInt(r.TsMs, 10) + "@" + r.RetentionClass
}

// ParseCallRef reads the text form of a reference.
func ParseCallRef(text string) (CallRef, error) {
	text = strings.TrimSpace(text)
	rest, class, ok := cutLast(text, "@")
	if !ok {
		return CallRef{}, errors.Errorf("%q is not a call reference from the calls operation", text)
	}
	pkText, tsText, ok := cutLast(rest, "@")
	if !ok {
		return CallRef{}, errors.Errorf("%q is not a call reference from the calls operation", text)
	}
	if !model.IsRetentionClass(class) {
		return CallRef{}, errors.Errorf("call reference %q: unknown retention class %q", text, class)
	}
	ts, err := strconv.ParseInt(tsText, 10, 64)
	if err != nil {
		return CallRef{}, errors.Errorf("call reference %q: bad time %q", text, tsText)
	}
	pk, err := model.ParsePKPath(pkText)
	if err != nil {
		return CallRef{}, errors.Wrapf(err, "call reference %q", text)
	}
	return CallRef{PK: pk, TsMs: ts, RetentionClass: class}, nil
}

func cutLast(s, sep string) (before, after string, ok bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}
