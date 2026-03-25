package pg

import (
	"fmt"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"slices"
)

type Params struct {
	ConnStr        string
	SSLMode        string
	CAFile         string
	SkipMonitoring bool
}

// IsEmpty checks if essential connection parameters are provided
func (pp Params) IsEmpty() bool {
	return pp.ConnStr == ""
}

// IsValid performs validation of all parameters
func (pp *Params) IsValid() error {
	if pp.IsEmpty() {
		return fmt.Errorf("some of required parameters are empty")
	}
	if !slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, pp.SSLMode) {
		return fmt.Errorf("incorrect SSL mode %s: should be \"disable\", \"allow\", \"prefer\", \"require\", \"verify-ca\" or \"verify-full\"", pp.SSLMode)
	}
	if pp.CAFile != "" {
		return files.CheckFile(pp.CAFile)
	}
	return nil
}
