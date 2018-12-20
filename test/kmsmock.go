package test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"io"
)

var plaintTextKeys = []byte{55, 85, 145, 172, 39, 36, 22, 82, 172, 169, 243, 229, 20, 201, 150, 85, 108, 131, 94, 158, 249, 235, 14, 228, 31, 106, 82, 144, 180, 91, 16, 172}
var cipherTextKeys = []byte{1, 2, 3, 0, 120, 117, 254, 40, 232, 55, 217, 84, 174, 75, 59, 2, 126, 80, 228, 40, 30, 154, 9, 181, 250, 93, 124, 148, 204, 162, 114, 168, 221, 48, 205, 156, 51, 1, 175, 64, 33, 234, 59, 118, 135, 116, 69, 35, 55, 119, 205, 97, 34, 121, 0, 0, 0, 126, 48, 124, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 6, 160, 111, 48, 109, 2, 1, 0, 48, 104, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 1, 48, 30, 6, 9, 96, 134, 72, 1, 101, 3, 4, 1, 46, 48, 17, 4, 12, 206, 50, 137, 217, 81, 233, 59, 171, 93, 91, 58, 132, 2, 1, 16, 128, 59, 113, 1, 196, 78, 149, 34, 228, 82, 195, 190, 192, 248, 95, 192, 11, 108, 200, 117, 57, 165, 152, 54, 95, 169, 176, 53, 71, 217, 119, 34, 10, 26, 143, 240, 124, 202, 53, 167, 94, 151, 240, 14, 124, 48, 154, 153, 11, 46, 189, 202, 163, 21, 232, 230, 10, 202, 105, 98, 175}

// KmsMock can be used to mock KMS during testing. Set up with a static key
type KmsMock struct {
	kmsiface.KMSAPI

	GenerateDataKeyCalledCount int
	DecryptCalledCount         int
}

// GenerateDataKey mock
func (k *KmsMock) GenerateDataKey(gki *kms.GenerateDataKeyInput) (*kms.GenerateDataKeyOutput, error) {
	k.GenerateDataKeyCalledCount++
	return &kms.GenerateDataKeyOutput{
		CiphertextBlob: cipherTextKeys,
		KeyId:          aws.String("keyID"),
		Plaintext:      plaintTextKeys,
	}, nil
}

// Decrypt mock
func (k *KmsMock) Decrypt(di *kms.DecryptInput) (*kms.DecryptOutput, error) {
	k.DecryptCalledCount++
	return &kms.DecryptOutput{
		KeyId:     aws.String("keyID"),
		Plaintext: plaintTextKeys,
	}, nil
}

// EncryptData is a helper function for encrypting during testing
func EncryptData(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(plaintTextKeys)
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

// DecryptData is a helper function for decrypting during testing
func DecryptData(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(plaintTextKeys)
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
