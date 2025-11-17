package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/xdg-go/scram"
)

// HashGeneratorFcn is a function type for generating hash functions
type HashGeneratorFcn func() hash.Hash

var (
	// SHA256 is the hash generator for SHA-256
	SHA256 HashGeneratorFcn = sha256.New

	// SHA512 is the hash generator for SHA-512
	SHA512 HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient implements the SCRAM client for Sarama
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn HashGeneratorFcn
}

// Begin starts the SCRAM conversation
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.scram(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step processes a step in the SCRAM conversation
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return response, err
}

// Done returns true when the conversation is complete
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// scram creates a SCRAM client
func (f HashGeneratorFcn) scram(userName, password, authzID string) (*scram.Client, error) {
	var hashGen scram.HashGeneratorFcn
	switch f {
	case SHA256:
		hashGen = func() hash.Hash { return sha256.New() }
	case SHA512:
		hashGen = func() hash.Hash { return sha512.New() }
	default:
		hashGen = func() hash.Hash { return sha256.New() }
	}

	return scram.NewClient(hashGen, userName, password, authzID)
}
