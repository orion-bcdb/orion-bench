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
	fmtUser      = "user-%s"
	fmtServer    = "server-%s"
	fmtUserIndex = "U%05d"
	fmtSubject   = "Orion cert for %s"
	perm         = 0766
	userHost     = "127.0.0.1"
)

type CryptoMaterial struct {
	lg *logger.SugarLogger

	name   string
	path   string
	cert   *x509.Certificate
	signer crypto.Signer
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

func (u *CryptoMaterial) generate(rootCA tls.Certificate, host string) {
	pemCert, privKey, err := testutils.IssueCertificate(u.subject(), host, rootCA)
	u.Check(err)
	u.write(pemCert, privKey)
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

func (u *CryptoMaterial) Cert() *x509.Certificate {
	if u.cert != nil {
		return u.cert
	}

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
