package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

// SecurityConfig holds comprehensive security configuration
type SecurityConfig struct {
	// SASL Configuration
	SASL *SASLConfig

	// TLS/SSL Configuration
	TLS *TLSConfig

	// Additional security settings
	EnableEncryption bool
	MinVersion       uint16 // Minimum TLS version
}

// SASLConfig holds SASL authentication configuration
type SASLConfig struct {
	// Enable SASL
	Enabled bool

	// SASL Mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER
	Mechanism SASLMechanism

	// Username and password for PLAIN, SCRAM
	Username string
	Password string

	// GSSAPI/Kerberos configuration
	GSSAPI *GSSAPIConfig

	// OAUTHBEARER configuration
	OAuth *OAuthConfig

	// Handshake determines whether to send the Kafka SASL handshake
	Handshake bool

	// Version is the SASL client version
	Version int16
}

// SASLMechanism defines the SASL authentication mechanism
type SASLMechanism string

const (
	SASLTypePlaintext     SASLMechanism = "PLAIN"
	SASLTypeSCRAMSHA256   SASLMechanism = "SCRAM-SHA-256"
	SASLTypeSCRAMSHA512   SASLMechanism = "SCRAM-SHA-512"
	SASLTypeGSSAPI        SASLMechanism = "GSSAPI"
	SASLTypeOAuthBearer   SASLMechanism = "OAUTHBEARER"
)

// GSSAPIConfig holds Kerberos/GSSAPI configuration
type GSSAPIConfig struct {
	AuthType           int
	KeyTabPath         string
	KerberosConfigPath string
	ServiceName        string
	Username           string
	Password           string
	Realm              string
	DisablePAFXFAST    bool
}

// OAuthConfig holds OAuth Bearer configuration
type OAuthConfig struct {
	TokenProvider func() (string, error)
	Extensions    map[string]string
}

// TLSConfig holds TLS/SSL configuration
type TLSConfig struct {
	// Enable TLS
	Enabled bool

	// Certificate files
	CertFile string
	KeyFile  string
	CAFile   string

	// Certificate data (alternative to files)
	CertData []byte
	KeyData  []byte
	CAData   []byte

	// TLS settings
	InsecureSkipVerify bool
	ServerName         string
	MinVersion         uint16
	MaxVersion         uint16

	// Client authentication
	ClientAuth tls.ClientAuthType

	// Cipher suites
	CipherSuites []uint16
}

// DefaultSecurityConfig returns a default security configuration
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		EnableEncryption: false,
		MinVersion:       tls.VersionTLS12,
	}
}

// NewSASLPlainConfig creates SASL PLAIN configuration
func NewSASLPlainConfig(username, password string) *SASLConfig {
	return &SASLConfig{
		Enabled:   true,
		Mechanism: SASLTypePlaintext,
		Username:  username,
		Password:  password,
		Handshake: true,
		Version:   1,
	}
}

// NewSASLSCRAMConfig creates SASL SCRAM configuration
func NewSASLSCRAMConfig(mechanism SASLMechanism, username, password string) *SASLConfig {
	if mechanism != SASLTypeSCRAMSHA256 && mechanism != SASLTypeSCRAMSHA512 {
		mechanism = SASLTypeSCRAMSHA256
	}

	return &SASLConfig{
		Enabled:   true,
		Mechanism: mechanism,
		Username:  username,
		Password:  password,
		Handshake: true,
		Version:   1,
	}
}

// NewSASLGSSAPIConfig creates SASL GSSAPI (Kerberos) configuration
func NewSASLGSSAPIConfig(serviceName, realm, username, password, keytabPath, kerberosConfigPath string) *SASLConfig {
	return &SASLConfig{
		Enabled:   true,
		Mechanism: SASLTypeGSSAPI,
		Handshake: true,
		GSSAPI: &GSSAPIConfig{
			ServiceName:        serviceName,
			Realm:              realm,
			Username:           username,
			Password:           password,
			KeyTabPath:         keytabPath,
			KerberosConfigPath: kerberosConfigPath,
			AuthType:           1, // User auth
			DisablePAFXFAST:    false,
		},
	}
}

// NewSASLOAuthConfig creates SASL OAuth Bearer configuration
func NewSASLOAuthConfig(tokenProvider func() (string, error)) *SASLConfig {
	return &SASLConfig{
		Enabled:   true,
		Mechanism: SASLTypeOAuthBearer,
		Handshake: true,
		OAuth: &OAuthConfig{
			TokenProvider: tokenProvider,
			Extensions:    make(map[string]string),
		},
	}
}

// NewTLSConfig creates a new TLS configuration
func NewTLSConfig(certFile, keyFile, caFile string) *TLSConfig {
	return &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		MinVersion: tls.VersionTLS12,
	}
}

// NewMutualTLSConfig creates mutual TLS configuration
func NewMutualTLSConfig(certFile, keyFile, caFile string) *TLSConfig {
	return &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}
}

// ApplyToSaramaConfig applies security configuration to Sarama config
func (sc *SecurityConfig) ApplyToSaramaConfig(config *sarama.Config) error {
	// Apply SASL configuration
	if sc.SASL != nil && sc.SASL.Enabled {
		if err := sc.applySASL(config); err != nil {
			return fmt.Errorf("failed to apply SASL config: %w", err)
		}
	}

	// Apply TLS configuration
	if sc.TLS != nil && sc.TLS.Enabled {
		tlsConfig, err := sc.buildTLSConfig()
		if err != nil {
			return fmt.Errorf("failed to build TLS config: %w", err)
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	return nil
}

func (sc *SecurityConfig) applySASL(config *sarama.Config) error {
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = sc.SASL.Handshake

	switch sc.SASL.Mechanism {
	case SASLTypePlaintext:
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = sc.SASL.Username
		config.Net.SASL.Password = sc.SASL.Password

	case SASLTypeSCRAMSHA256:
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.User = sc.SASL.Username
		config.Net.SASL.Password = sc.SASL.Password
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}

	case SASLTypeSCRAMSHA512:
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.User = sc.SASL.Username
		config.Net.SASL.Password = sc.SASL.Password
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}

	case SASLTypeGSSAPI:
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		if sc.SASL.GSSAPI != nil {
			config.Net.SASL.GSSAPI.AuthType = sc.SASL.GSSAPI.AuthType
			config.Net.SASL.GSSAPI.KeyTabPath = sc.SASL.GSSAPI.KeyTabPath
			config.Net.SASL.GSSAPI.KerberosConfigPath = sc.SASL.GSSAPI.KerberosConfigPath
			config.Net.SASL.GSSAPI.ServiceName = sc.SASL.GSSAPI.ServiceName
			config.Net.SASL.GSSAPI.Username = sc.SASL.GSSAPI.Username
			config.Net.SASL.GSSAPI.Password = sc.SASL.GSSAPI.Password
			config.Net.SASL.GSSAPI.Realm = sc.SASL.GSSAPI.Realm
			config.Net.SASL.GSSAPI.DisablePAFXFAST = sc.SASL.GSSAPI.DisablePAFXFAST
		}

	case SASLTypeOAuthBearer:
		config.Net.SASL.Mechanism = sarama.SASLTypeOAuthBearer
		if sc.SASL.OAuth != nil {
			config.Net.SASL.TokenProvider = &customTokenProvider{
				tokenFunc: sc.SASL.OAuth.TokenProvider,
			}
		}

	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", sc.SASL.Mechanism)
	}

	if sc.SASL.Version > 0 {
		config.Net.SASL.Version = sc.SASL.Version
	}

	return nil
}

func (sc *SecurityConfig) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: sc.TLS.InsecureSkipVerify,
		ServerName:         sc.TLS.ServerName,
		MinVersion:         sc.TLS.MinVersion,
		MaxVersion:         sc.TLS.MaxVersion,
		ClientAuth:         sc.TLS.ClientAuth,
		CipherSuites:       sc.TLS.CipherSuites,
	}

	// Set minimum version
	if tlsConfig.MinVersion == 0 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}

	// Load CA certificate
	if sc.TLS.CAFile != "" || len(sc.TLS.CAData) > 0 {
		caCertPool := x509.NewCertPool()

		var caData []byte
		var err error

		if sc.TLS.CAFile != "" {
			caData, err = os.ReadFile(sc.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file: %w", err)
			}
		} else {
			caData = sc.TLS.CAData
		}

		if !caCertPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate
	if sc.TLS.CertFile != "" && sc.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(sc.TLS.CertFile, sc.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	} else if len(sc.TLS.CertData) > 0 && len(sc.TLS.KeyData) > 0 {
		cert, err := tls.X509KeyPair(sc.TLS.CertData, sc.TLS.KeyData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Validate validates the security configuration
func (sc *SecurityConfig) Validate() error {
	if sc.SASL != nil && sc.SASL.Enabled {
		if err := sc.validateSASL(); err != nil {
			return err
		}
	}

	if sc.TLS != nil && sc.TLS.Enabled {
		if err := sc.validateTLS(); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SecurityConfig) validateSASL() error {
	switch sc.SASL.Mechanism {
	case SASLTypePlaintext, SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512:
		if sc.SASL.Username == "" {
			return fmt.Errorf("SASL username is required")
		}
		if sc.SASL.Password == "" {
			return fmt.Errorf("SASL password is required")
		}

	case SASLTypeGSSAPI:
		if sc.SASL.GSSAPI == nil {
			return fmt.Errorf("GSSAPI configuration is required")
		}
		if sc.SASL.GSSAPI.ServiceName == "" {
			return fmt.Errorf("GSSAPI service name is required")
		}
		if sc.SASL.GSSAPI.Realm == "" {
			return fmt.Errorf("GSSAPI realm is required")
		}

	case SASLTypeOAuthBearer:
		if sc.SASL.OAuth == nil || sc.SASL.OAuth.TokenProvider == nil {
			return fmt.Errorf("OAuth token provider is required")
		}

	default:
		return fmt.Errorf("unknown SASL mechanism: %s", sc.SASL.Mechanism)
	}

	return nil
}

func (sc *SecurityConfig) validateTLS() error {
	if sc.TLS.CertFile != "" && sc.TLS.KeyFile == "" {
		return fmt.Errorf("key file is required when cert file is provided")
	}

	if sc.TLS.KeyFile != "" && sc.TLS.CertFile == "" {
		return fmt.Errorf("cert file is required when key file is provided")
	}

	if len(sc.TLS.CertData) > 0 && len(sc.TLS.KeyData) == 0 {
		return fmt.Errorf("key data is required when cert data is provided")
	}

	if len(sc.TLS.KeyData) > 0 && len(sc.TLS.CertData) == 0 {
		return fmt.Errorf("cert data is required when key data is provided")
	}

	// Validate TLS version
	if sc.TLS.MinVersion != 0 {
		if sc.TLS.MinVersion < tls.VersionTLS12 {
			return fmt.Errorf("minimum TLS version should be TLS 1.2 or higher")
		}
	}

	if sc.TLS.MaxVersion != 0 {
		if sc.TLS.MaxVersion < sc.TLS.MinVersion {
			return fmt.Errorf("max TLS version cannot be less than min version")
		}
	}

	return nil
}

// customTokenProvider implements sarama.AccessTokenProvider
type customTokenProvider struct {
	tokenFunc func() (string, error)
}

func (p *customTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := p.tokenFunc()
	if err != nil {
		return nil, err
	}

	return &sarama.AccessToken{
		Token: token,
	}, nil
}

// SecureConfigBuilder helps build secure configurations
type SecureConfigBuilder struct {
	config *Config
	security *SecurityConfig
}

// NewSecureConfigBuilder creates a new secure config builder
func NewSecureConfigBuilder() *SecureConfigBuilder {
	return &SecureConfigBuilder{
		config:   DefaultConfig(),
		security: DefaultSecurityConfig(),
	}
}

// WithBrokers sets the Kafka brokers
func (scb *SecureConfigBuilder) WithBrokers(brokers []string) *SecureConfigBuilder {
	scb.config.Brokers = brokers
	return scb
}

// WithConsumerGroup sets the consumer group
func (scb *SecureConfigBuilder) WithConsumerGroup(group string) *SecureConfigBuilder {
	scb.config.ConsumerGroup = group
	return scb
}

// WithSASLPlain enables SASL PLAIN authentication
func (scb *SecureConfigBuilder) WithSASLPlain(username, password string) *SecureConfigBuilder {
	scb.security.SASL = NewSASLPlainConfig(username, password)
	return scb
}

// WithSASLSCRAM enables SASL SCRAM authentication
func (scb *SecureConfigBuilder) WithSASLSCRAM(mechanism SASLMechanism, username, password string) *SecureConfigBuilder {
	scb.security.SASL = NewSASLSCRAMConfig(mechanism, username, password)
	return scb
}

// WithTLS enables TLS encryption
func (scb *SecureConfigBuilder) WithTLS(certFile, keyFile, caFile string) *SecureConfigBuilder {
	scb.security.TLS = NewTLSConfig(certFile, keyFile, caFile)
	return scb
}

// WithMutualTLS enables mutual TLS authentication
func (scb *SecureConfigBuilder) WithMutualTLS(certFile, keyFile, caFile string) *SecureConfigBuilder {
	scb.security.TLS = NewMutualTLSConfig(certFile, keyFile, caFile)
	return scb
}

// WithInsecureSkipVerify disables TLS verification (not recommended for production)
func (scb *SecureConfigBuilder) WithInsecureSkipVerify() *SecureConfigBuilder {
	if scb.security.TLS != nil {
		scb.security.TLS.InsecureSkipVerify = true
	}
	return scb
}

// Build builds the configuration
func (scb *SecureConfigBuilder) Build() (*Config, error) {
	// Validate security config
	if err := scb.security.Validate(); err != nil {
		return nil, fmt.Errorf("security validation failed: %w", err)
	}

	// Create Sarama config
	saramaConfig, err := scb.config.ToSaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama config: %w", err)
	}

	// Apply security settings
	if err := scb.security.ApplyToSaramaConfig(saramaConfig); err != nil {
		return nil, fmt.Errorf("failed to apply security config: %w", err)
	}

	return scb.config, nil
}

// Environment variable configuration loader
type EnvConfigLoader struct{}

// LoadFromEnv loads security configuration from environment variables
func (ecl *EnvConfigLoader) LoadFromEnv() *SecurityConfig {
	config := DefaultSecurityConfig()

	// SASL configuration
	if os.Getenv("KAFKA_SASL_ENABLED") == "true" {
		mechanism := SASLMechanism(os.Getenv("KAFKA_SASL_MECHANISM"))
		username := os.Getenv("KAFKA_SASL_USERNAME")
		password := os.Getenv("KAFKA_SASL_PASSWORD")

		config.SASL = &SASLConfig{
			Enabled:   true,
			Mechanism: mechanism,
			Username:  username,
			Password:  password,
			Handshake: true,
			Version:   1,
		}
	}

	// TLS configuration
	if os.Getenv("KAFKA_TLS_ENABLED") == "true" {
		config.TLS = &TLSConfig{
			Enabled:            true,
			CertFile:           os.Getenv("KAFKA_TLS_CERT_FILE"),
			KeyFile:            os.Getenv("KAFKA_TLS_KEY_FILE"),
			CAFile:             os.Getenv("KAFKA_TLS_CA_FILE"),
			InsecureSkipVerify: os.Getenv("KAFKA_TLS_SKIP_VERIFY") == "true",
			ServerName:         os.Getenv("KAFKA_TLS_SERVER_NAME"),
			MinVersion:         tls.VersionTLS12,
		}
	}

	return config
}
