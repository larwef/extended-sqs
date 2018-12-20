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

func (s *s3Client) putObject(bucket *string, payload *string) (*fileEvent, error) {
	key := uuid.New().String()
	poi := &s3.PutObjectInput{
		Body:   bytes.NewReader([]byte(*payload)),
		Bucket: bucket,
		Key:    &key,
	}

	fe := &fileEvent{
		Size:     aws.Int64(int64(len(*payload))),
		Bucket:   bucket,
		Filename: &key,
	}

	_, err := s.awsS3.PutObject(poi)

	return fe, err
}

func (s *s3Client) getObject(fe *fileEvent) (string, error) {
	goi := &s3.GetObjectInput{
		Bucket: fe.Bucket,
		Key:    fe.Filename,
	}

	goo, err := s.awsS3.GetObject(goi)
	if err != nil {
		return "", err
	}

	payload, err := ioutil.ReadAll(goo.Body)
	return string(payload), err
}
