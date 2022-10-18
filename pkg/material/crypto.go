// Author: Liran Funaro <liran.funaro@ibm.com>

package material

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"orion-bench/pkg/utils"

	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
)

const (
	Root         = "root"
	Admin        = "admin"
	prefixUser   = "user-"
	prefixNode   = "node-"
	fmtUserIndex = "U%05d"
	fmtNodeIndex = "N%05d"
	fmtSubject   = "Orion %s CA"
	perm         = 0766
	userHost     = "127.0.0.1"
)

type CryptoMaterial struct {
	lg   *logger.SugarLogger
	name string
	path string

	// Evaluated lazily
	cert    *x509.Certificate
	signer  crypto.Signer
	keyPair *tls.Certificate
}

func (u *CryptoMaterial) Check(err error) {
	utils.Check(u.lg, err)
}

func (u *CryptoMaterial) subject() string {
	return fmt.Sprintf(fmtSubject, u.name)
}

func (u *CryptoMaterial) write(cert []byte, key []byte) {
	for filePath, data := range map[string][]byte{u.CertPath(): cert, u.KeyPath(): key} {
		u.Check(os.WriteFile(filePath, data, perm))
	}
}

func (u *CryptoMaterial) generateRoot() {
	if u.name != Root {
		u.lg.Fatalf("Attempt to generate root certificate with non root user (%s).", u.name)
	}
	pemCert, privKey, err := testutils.GenerateRootCA(u.subject(), userHost)
	u.Check(err)
	u.write(pemCert, privKey)
}

func (u *CryptoMaterial) generate(root *CryptoMaterial, host string) {
	if u.name == Root {
		u.lg.Fatalf("Attempt to generate non-root certificate with root user.")
	}
	pemCert, privKey, err := testutils.IssueCertificate(u.subject(), host, *root.KeyPair())
	u.Check(err)
	u.write(pemCert, privKey)
}

func (u *CryptoMaterial) Name() string {
	return u.name
}

func (u *CryptoMaterial) CertPath() string {
	return u.path + ".pem"
}

func (u *CryptoMaterial) KeyPath() string {
	return u.path + ".key"
}

func (u *CryptoMaterial) Config() *config.UserConfig {
	return &config.UserConfig{
		UserID:         u.name,
		CertPath:       u.CertPath(),
		PrivateKeyPath: u.KeyPath(),
	}
}

func (u *CryptoMaterial) TLS() config.ClientTLSConfig {
	return config.ClientTLSConfig{
		//ClientCertificatePath: u.CertPath(),
		//ClientKeyPath:         u.KeyPath(),
	}
}

func (u *CryptoMaterial) Cert() *x509.Certificate {
	b, err := os.ReadFile(u.CertPath())
	u.Check(err)
	bl, _ := pem.Decode(b)
	if bl == nil {
		u.lg.Fatalf("No certificate found in file: %s", u.CertPath())
	}
	certRaw := bl.Bytes
	cert, err := x509.ParseCertificate(certRaw)
	u.Check(err)

	u.cert = cert
	return cert
}

func (u *CryptoMaterial) Signer() crypto.Signer {
	if u.signer != nil {
		return u.signer
	}

	signer, err := crypto.NewSigner(&crypto.SignerOptions{
		Identity:    u.name,
		KeyFilePath: u.KeyPath(),
	})
	u.Check(err)

	u.signer = signer
	return signer
}

func (u *CryptoMaterial) KeyPair() *tls.Certificate {
	if u.keyPair != nil {
		return u.keyPair
	}
	keyPair, err := tls.LoadX509KeyPair(u.CertPath(), u.KeyPath())
	u.Check(err)

	u.keyPair = &keyPair
	return u.keyPair
}
