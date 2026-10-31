package http

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidToken       = errors.New("invalid token format")
	ErrUnsupportedAlgorithm = errors.New("unsupported jwt algorithm")
	ErrTokenExpired       = errors.New("token expired")
	ErrTokenNotYetValid   = errors.New("token not yet valid")
	ErrInvalidSignature   = errors.New("invalid signature")
	ErrMissingSubject     = errors.New("missing or invalid sub claim")
)

// RolesClaim represents roles that can be either an array of strings or a comma-separated string.
type RolesClaim []string

func (r *RolesClaim) UnmarshalJSON(data []byte) error {
	var list []string
	if err := json.Unmarshal(data, &list); err == nil {
		*r = list
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		parts := strings.Split(str, ",")
		res := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				res = append(res, trimmed)
			}
		}
		*r = res
		return nil
	}
	return nil
}

// JWTClaims represents standard and custom claims.
type JWTClaims struct {
	Sub   string     `json:"sub"`
	Roles RolesClaim `json:"roles"`
	Email string     `json:"email"`
	Exp   int64      `json:"exp"`
	Nbf   int64      `json:"nbf"`
	Iat   int64      `json:"iat"`
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// ParseAndValidateJWT parses a bearer JWT string, validates signature using HS256, RS256, or ES256,
// checks expiration and not-before claims, and returns parsed claims.
func ParseAndValidateJWT(tokenStr string, secret string, publicKeyPEM string) (*JWTClaims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid header encoding", ErrInvalidToken)
	}

	var header jwtHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("%w: invalid header json", ErrInvalidToken)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid payload encoding", ErrInvalidToken)
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid signature encoding", ErrInvalidToken)
	}

	signedContent := parts[0] + "." + parts[1]

	// Verify signature
	switch strings.ToUpper(header.Alg) {
	case "HS256":
		if secret == "" {
			return nil, errors.New("jwt secret is not configured for HS256 validation")
		}
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signedContent))
		expectedSig := mac.Sum(nil)
		if subtle.ConstantTimeCompare(sigBytes, expectedSig) != 1 {
			return nil, ErrInvalidSignature
		}

	case "RS256":
		if publicKeyPEM == "" {
			return nil, errors.New("jwt public key is not configured for RS256 validation")
		}
		rsaPub, err := parseRSAPublicKey(publicKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed parsing rsa public key: %w", err)
		}
		hashed := sha256.Sum256([]byte(signedContent))
		if err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, hashed[:], sigBytes); err != nil {
			return nil, ErrInvalidSignature
		}

	case "ES256":
		if publicKeyPEM == "" {
			return nil, errors.New("jwt public key is not configured for ES256 validation")
		}
		ecdsaPub, err := parseECDSAPublicKey(publicKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed parsing ecdsa public key: %w", err)
		}
		hashed := sha256.Sum256([]byte(signedContent))
		if !verifyES256Signature(ecdsaPub, hashed[:], sigBytes) {
			return nil, ErrInvalidSignature
		}

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedAlgorithm, header.Alg)
	}

	var claims JWTClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("%w: invalid payload json", ErrInvalidToken)
	}

	now := time.Now().Unix()

	// Expiration check
	if claims.Exp != 0 && now >= claims.Exp {
		return nil, ErrTokenExpired
	}

	// Not before check
	if claims.Nbf != 0 && now < claims.Nbf {
		return nil, ErrTokenNotYetValid
	}

	// Sub check
	if claims.Sub == "" {
		return nil, ErrMissingSubject
	}
	if _, err := uuid.Parse(claims.Sub); err != nil {
		return nil, fmt.Errorf("%w: sub must be valid uuid", ErrMissingSubject)
	}

	return &claims, nil
}

func parseRSAPublicKey(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("failed decoding PEM block for public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err == nil {
		if rsaPub, ok := pub.(*rsa.PublicKey); ok {
			return rsaPub, nil
		}
	}

	rsaPub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err == nil {
		return rsaPub, nil
	}

	return nil, errors.New("unable to parse rsa public key from PEM")
}

func parseECDSAPublicKey(pemStr string) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("failed decoding PEM block for public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	ecdsaPub, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("key is not an ecdsa public key")
	}
	return ecdsaPub, nil
}

func verifyES256Signature(pub *ecdsa.PublicKey, hash []byte, sig []byte) bool {
	// First try standard 64-byte IEEE P1363 format (R||S)
	if len(sig) == 64 {
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		if ecdsa.Verify(pub, hash, r, s) {
			return true
		}
	}
	// Fallback to ASN.1
	return ecdsa.VerifyASN1(pub, hash, sig)
}

// GenerateTestHS256Token creates a valid HS256 JWT string for unit tests.
func GenerateTestHS256Token(sub string, roles []string, email string, secret string, ttl time.Duration) (string, error) {
	header := jwtHeader{
		Alg: "HS256",
		Typ: "JWT",
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return "", err
	}

	now := time.Now().Unix()
	claims := JWTClaims{
		Sub:   sub,
		Roles: roles,
		Email: email,
		Iat:   now,
		Exp:   now + int64(ttl.Seconds()),
	}
	claimsBytes, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)
	payloadB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)
	signingInput := headerB64 + "." + payloadB64

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sigB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return signingInput + "." + sigB64, nil
}
