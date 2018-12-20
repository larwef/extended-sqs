package kitsune

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"io"
)

type encryptedEvent struct {
	EncryptedEncryptionKey []byte `json:"encryptedEncryptionKey"`
	KeyID                  string `json:"keyId"`
	Payload                []byte `json:"payload"`
}

type kmsClient struct {
	awsKMS kmsiface.KMSAPI
}

func (k *kmsClient) encrypt(keyID *string, payload []byte) (*encryptedEvent, error) {
	gki := &kms.GenerateDataKeyInput{
		KeyId:   keyID,
		KeySpec: aws.String(kms.DataKeySpecAes256),
	}

	gko, err := k.awsKMS.GenerateDataKey(gki)
	if err != nil {
		return nil, err
	}

	encryptedPayload, err := encryptData(payload, gko.Plaintext)

	encryptedEvent := &encryptedEvent{
		EncryptedEncryptionKey: gko.CiphertextBlob,
		KeyID:                  *gko.KeyId,
		Payload:                encryptedPayload,
	}

	return encryptedEvent, err
}

func (k *kmsClient) decrypt(ee *encryptedEvent) ([]byte, error) {
	di := &kms.DecryptInput{
		CiphertextBlob: ee.EncryptedEncryptionKey,
	}

	do, err := k.awsKMS.Decrypt(di)
	if err != nil {
		return nil, err
	}

	return decryptData(ee.Payload, do.Plaintext)

}

func encryptData(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

func decryptData(data []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
