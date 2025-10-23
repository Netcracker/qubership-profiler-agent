package s3

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/files"
)

type Params struct {
	Endpoint        string // host (without protocol)
	AccessKeyID     string // token
	SecretAccessKey string // secret
	UseSSL          bool   // use http or https protocol
	InsecureSSL     bool   // skip SSL cert validation
	Region          string
	ObjectLocking   bool
	BucketName      string // default bucket
	CAFile          string
}

func (p *Params) IsEmpty() bool {
	return strings.TrimSpace(p.Endpoint) == "" ||
		strings.TrimSpace(p.AccessKeyID) == "" ||
		strings.TrimSpace(p.SecretAccessKey) == ""
}

func (p *Params) Prepare() {

	// remove http/https and trail slash
	reProtocol := regexp.MustCompile(`^https?://`)
	p.Endpoint = reProtocol.ReplaceAllString(p.Endpoint, "")
	reTrailingSlash := regexp.MustCompile(`/$`)
	p.Endpoint = reTrailingSlash.ReplaceAllString(p.Endpoint, "")

	// if bucket name not provided use default bucket name
	if p.BucketName == "" {
		p.BucketName = "profiler"
	}
}

func (p *Params) IsValid() error {

	if p.IsEmpty() {
		return fmt.Errorf("some of required parameters for S3 are empty")
	}

	// check using prefix and trail slash
	regex := `^(http://|https://)|/$`
	re := regexp.MustCompile(regex)
	if re.MatchString(p.Endpoint) {
		return fmt.Errorf("s3 endpoint contains either a protocol or ends with a trailing slash")
	}

	if strings.TrimSpace(p.BucketName) == "" {
		return fmt.Errorf("empty backet name")
	}
	if p.InsecureSSL && !p.UseSSL {
		return fmt.Errorf("insecure SSL is specified for not tls connection")
	}
	if p.CAFile != "" && !p.UseSSL {
		return fmt.Errorf("custom CA is specified for not tls connection")
	}
	if p.CAFile != "" {
		return files.CheckFile(p.CAFile)
	}
	return nil
}
