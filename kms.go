package kitsune

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"io"
)

type kmsClient struct {
	opts   *options
	awsKMS kmsiface.KMSAPI
}

func (k *kmsClient) Encrypt(payload []byte) ([]byte, error) {
	gki := &kms.GenerateDataKeyInput{
		KeyId:   aws.String("alias/sqs-client-test-key"),
		KeySpec: aws.String(kms.DataKeySpecAes256),
	}

	gko, err := k.awsKMS.GenerateDataKey(gki)
	if err != nil {
		return nil, err
	}

	encryptedPayload, err := encrypt(payload, gko.Plaintext)
	if err != nil {
		return nil, err
	}

	encryptedEvent := encryptedEvent{
		EncryptedEncryptionKey: gko.CiphertextBlob,
		KeyID:                  *gko.KeyId,
		Payload:                encryptedPayload,
	}

	return json.Marshal(encryptedEvent)
}

func (k *kmsClient) Decrypt(payload []byte) ([]byte, error) {
	var ee encryptedEvent
	if err := json.Unmarshal(payload, &ee); err != nil {
		return nil, err
	}

	di := &kms.DecryptInput{
		CiphertextBlob: ee.EncryptedEncryptionKey,
	}

	do, err := k.awsKMS.Decrypt(di)
	if err != nil {
		return nil, err
	}

	return decrypt(ee.Payload, do.Plaintext)

}

func encrypt(data []byte, key []byte) ([]byte, error) {
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

func decrypt(data []byte, key []byte) ([]byte, error) {
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
