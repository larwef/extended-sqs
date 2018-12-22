package kitsune

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"
	"io/ioutil"
)

type fileEvent struct {
	Size     *int64  `json:"size,omitempty"`
	Bucket   *string `json:"bucket,omitempty"`
	Filename *string `json:"filename,omitempty"`
}

type s3Client struct {
	awsS3 s3iface.S3API
}

// TODO: Consider another naming convention for files
func (s *s3Client) putObject(bucket *string, payload []byte) (*fileEvent, error) {
	key := uuid.New().String()
	poi := &s3.PutObjectInput{
		Body:   bytes.NewReader(payload),
		Bucket: bucket,
		Key:    &key,
	}

	fe := &fileEvent{
		Size:     aws.Int64(int64(len(payload))),
		Bucket:   bucket,
		Filename: &key,
	}

	_, err := s.awsS3.PutObject(poi)

	return fe, err
}

func (s *s3Client) getObject(fe *fileEvent) ([]byte, error) {
	goi := &s3.GetObjectInput{
		Bucket: fe.Bucket,
		Key:    fe.Filename,
	}

	goo, err := s.awsS3.GetObject(goi)
	if err != nil {
		return nil, err
	}
	defer goo.Body.Close()

	payload, err := ioutil.ReadAll(goo.Body)
	return payload, err
}
