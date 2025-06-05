package utils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	TLS_CERT_DIR = "TLS_CERT_DIR"
)

func GetTlsDetails() (*tls.Config, error) {
	certDir := os.Getenv(TLS_CERT_DIR)
	if certDir != "" {

		certFile := filepath.Join(certDir, "tls.crt")
		keyFile := filepath.Join(certDir, "tls.key")
		caCertFile := filepath.Join(certDir, "ca.crt")

		serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			fmt.Println("Error loading server certificate:", err)
			return nil, err
		}

		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			fmt.Println("Error reading CA certificate:", err)
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			RootCAs:      caCertPool,
		}
		return tlsConfig, nil
	} else {
		return &tls.Config{}, nil
	}
}

func HttpClient() (*http.Client, error) {
	tlsConfig, err := GetTlsDetails()

	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	return client, nil
}
