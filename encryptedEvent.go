package kitsune

type encryptedEvent struct {
	EncryptedEncryptionKey []byte `json:"encryptedEncryptionKey"`
	KeyID                  string `json:"keyId"`
	Payload                []byte `json:"payload"`
}
