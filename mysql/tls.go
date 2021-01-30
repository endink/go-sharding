/*
 * Copyright 2021. Go-Sharding Author All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  File author: Anders Xiao
 */

package mysql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"strings"
	"sync"
	"time"
)

// Updated list of acceptable cipher suits to address
// Fixed upstream in https://github.com/golang/go/issues/13385
// This removed CBC mode ciphers that are suseptiable to Lucky13 style attacks
func newTLSConfig() *tls.Config {
	return &tls.Config{
		// MySQL Community edition has some problems with TLS1.2
		// TODO: Validate this will not break servers using mysql community edition < 5.7.10
		// MinVersion: tls.VersionTLS12,

		// Default ordering taken from
		// go 1.11 crypto/tls/cipher_suites.go
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}
}

var onceByKeys = sync.Map{}

// ClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func ClientTlsConfig(cert, key, ca, name string) (*tls.Config, error) {
	config := newTLSConfig()

	// Load the client-side cert & key if any.
	if cert != "" && key != "" {
		certificates, err := loadTLSCertificate(cert, key)

		if err != nil {
			return nil, err
		}

		config.Certificates = *certificates
	}

	// Load the server CA if any.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)

		if err != nil {
			return nil, err
		}

		config.RootCAs = certificatePool
	}

	// Set the server name if any.
	if name != "" {
		config.ServerName = name
	}

	return config, nil
}

type TLSConfig struct {
	Config    *tls.Config
	PublicKey []byte
}

func GenTLSConfig() *TLSConfig {
	caPem, caKey := generateCA()
	certPem, keyPem := generateAndSignRSACerts(caPem, caKey)
	config := NewServerTLSConfigFromPem(caPem, certPem, keyPem)
	pubKey := getPublicKeyFromCert(certPem)
	return &TLSConfig{
		Config:    config,
		PublicKey: pubKey,
	}
}

func NewServerTLSConfig() *tls.Config {
	caPem, caKey := generateCA()
	certPem, keyPem := generateAndSignRSACerts(caPem, caKey)
	return NewServerTLSConfigFromPem(caPem, certPem, keyPem)
}

func NewServerTLSConfigFromPem(caPem, certPem, keyPem []byte) *tls.Config {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		panic("failed to add ca PEM")
	}

	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		panic(err)
	}

	config := &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
	}
	return config
}

// ServerConfig returns the TLS config to use for a server to
// accept client connections.
func NewServerTLSConfigFromFile(certFile, keyFile, ca string) (*tls.Config, error) {
	config := newTLSConfig()

	certificates, err := loadTLSCertificate(certFile, keyFile)

	if err != nil {
		return nil, err
	}

	config.Certificates = *certificates

	// if specified, load ca to validate client,
	// and enforce clients present valid certs.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)

		if err != nil {
			return nil, err
		}

		config.ClientCAs = certificatePool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return config, nil
}

var certPools = sync.Map{}

func loadx509CertPool(ca string) (*x509.CertPool, error) {
	once, _ := onceByKeys.LoadOrStore(ca, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadx509CertPool(ca)
	})
	if err != nil {
		return nil, err
	}

	result, ok := certPools.Load(ca)

	if !ok {
		return nil, fmt.Errorf("Cannot find loaded x509 cert pool for ca: %s", ca)
	}

	return result.(*x509.CertPool), nil
}

func doLoadx509CertPool(ca string) error {
	b, err := ioutil.ReadFile(ca)
	if err != nil {
		return fmt.Errorf("failed to read ca file: %s", ca)
	}

	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return fmt.Errorf("failed to append certificates")
	}

	certPools.Store(ca, cp)

	return nil
}

var tlsCertificates = sync.Map{}

func tlsCertificatesIdentifier(cert, key string) string {
	return strings.Join([]string{cert, key}, ";")
}

func loadTLSCertificate(cert, key string) (*[]tls.Certificate, error) {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)
	once, _ := onceByKeys.LoadOrStore(tlsIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadTLSCertificate(cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := tlsCertificates.Load(tlsIdentifier)

	if !ok {
		return nil, fmt.Errorf("Cannot find loaded tls certificate with cert: %s, key%s", cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadTLSCertificate(cert, key string) error {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)

	var certificate []tls.Certificate
	// Load the server cert and key.
	crt, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return fmt.Errorf("failed to load tls certificate, cert %s, key: %s", cert, key)
	}

	certificate = []tls.Certificate{crt}

	tlsCertificates.Store(tlsIdentifier, &certificate)

	return nil
}

func genPublicKeyFromTlsConfig(config *tls.Config) ([]byte, error) {
	cert := config.Certificates[0]
	pubKey, err := x509.MarshalPKIXPublicKey(cert.Leaf.PublicKey)
	return pubKey, err
}

// extract RSA public key from certificate
func getPublicKeyFromCert(certPem []byte) []byte {
	block, _ := pem.Decode(certPem)
	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		panic(err)
	}
	pubKey, err := x509.MarshalPKIXPublicKey(crt.PublicKey.(*rsa.PublicKey))
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubKey})
}

// generate and sign RSA certificates with given CA
// see: https://fale.io/blog/2017/06/05/create-a-pki-in-golang/
func generateAndSignRSACerts(caPem, caKey []byte) ([]byte, []byte) {
	// Load CA
	catls, err := tls.X509KeyPair(caPem, caKey)
	if err != nil {
		panic(err)
	}
	ca, err := x509.ParseCertificate(catls.Certificate[0])
	if err != nil {
		panic(err)
	}

	// use the CA to sign certificates
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(err)
	}
	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	// sign the certificate
	cert_b, err := x509.CreateCertificate(rand.Reader, ca, cert, &priv.PublicKey, catls.PrivateKey)
	if err != nil {
		panic(err)
	}
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert_b})
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPem, keyPem
}

// generate CA in PEM
// see: https://github.com/golang/go/blob/master/src/crypto/tls/generate_cert.go
func generateCA() ([]byte, []byte) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
	}

	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		panic(err)
	}

	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	caKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return caPem, caKey
}
