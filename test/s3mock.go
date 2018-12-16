package test

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// S3Mock can be used to mock S3 during testing. Inject your own handlers.
type S3Mock struct {
	s3iface.S3API

	PutObjectHandler            func(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	PutObjectHandlerCalledCount int

	GetObjectHandler            func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	GetObjectHandlerCalledCount int
}

// PutObject mocks S3 PutObject. Call the handler configured for the S3Mock object.
func (s *S3Mock) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	s.PutObjectHandlerCalledCount++
	return s.PutObjectHandler(poi)
}

// GetObject mocks S3 GetObject. Call the handler configured for the S3Mock object.
func (s *S3Mock) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	s.GetObjectHandlerCalledCount++
	return s.GetObjectHandler(goi)
}
