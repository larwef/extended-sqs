package kitsune

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"io"
	"time"
)

type encryptedEvent struct {
	EncryptedEncryptionKey []byte `json:"encryptedEncryptionKey"`
	KeyID                  string `json:"keyId"`
	Payload                []byte `json:"payload"`
}

type keyCache struct {
	entries          map[[16]byte]cacheEntry
	expirationPeriod time.Duration
}

type cacheEntry struct {
	plainText  []byte
	cipherText []byte
	entered    time.Time
}

func (kc *keyCache) put(key [16]byte, plainText []byte, cipherText []byte) {
	kc.clean()

	kc.entries[key] = cacheEntry{
		plainText:  plainText,
		cipherText: cipherText,
		entered:    time.Now(),
	}
}

func (kc *keyCache) get(key [16]byte) (cacheEntry, bool) {
	kc.clean()

	entry, exists := kc.entries[key]
	return entry, exists
}

func (kc *keyCache) clean() {
	for key, value := range kc.entries {
		if value.entered.Add(kc.expirationPeriod).Before(time.Now()) {
			delete(kc.entries, key)
		}
	}
}

type kmsClient struct {
	cache        keyCache
	cacheEnabled bool
	awsKMS       kmsiface.KMSAPI
}

func (k *kmsClient) encrypt(keyID *string, payload []byte) (*encryptedEvent, error) {
	gki := &kms.GenerateDataKeyInput{
		KeyId:   keyID,
		KeySpec: aws.String(kms.DataKeySpecAes256),
	}

	gko, err := k.generateDataKey(gki)
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

func (k *kmsClient) generateDataKey(gki *kms.GenerateDataKeyInput) (*kms.GenerateDataKeyOutput, error) {
	if entry, exists := k.cache.get(md5.Sum([]byte(*gki.KeyId))); exists {
		return &kms.GenerateDataKeyOutput{
			CiphertextBlob: entry.cipherText,
			KeyId:          gki.KeyId,
			Plaintext:      entry.plainText,
		}, nil
	}

	gko, err := k.awsKMS.GenerateDataKey(gki)
	if k.cacheEnabled && err == nil {
		// Put with both key name and ciphertext so it wont need to get its own key for decryption
		k.cache.put(md5.Sum([]byte(*gki.KeyId)), gko.Plaintext, gko.CiphertextBlob)
		k.cache.put(md5.Sum(gko.CiphertextBlob), gko.Plaintext, gko.CiphertextBlob)
	}

	return gko, err
}

func (k *kmsClient) decrypt(ee *encryptedEvent) ([]byte, error) {
	di := &kms.DecryptInput{
		CiphertextBlob: ee.EncryptedEncryptionKey,
	}

	do, err := k.fetchKey(di)
	if err != nil {
		return nil, err
	}

	return decryptData(ee.Payload, do.Plaintext)
}

func (k *kmsClient) fetchKey(di *kms.DecryptInput) (*kms.DecryptOutput, error) {
	if entry, exists := k.cache.get(md5.Sum(di.CiphertextBlob)); exists {
		return &kms.DecryptOutput{
			Plaintext: entry.plainText,
		}, nil
	}

	do, err := k.awsKMS.Decrypt(di)
	if k.cacheEnabled && err == nil {
		k.cache.put(md5.Sum(di.CiphertextBlob), do.Plaintext, di.CiphertextBlob)
	}

	return do, err
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
