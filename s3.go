package kitsune

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io/ioutil"
)

type s3Client struct {
	awsS3 s3iface.S3API
}

func (s *s3Client) putObject(bucket string, key string, payload string) error {
	poi := &s3.PutObjectInput{
		Body:   bytes.NewReader([]byte(payload)),
		Bucket: &bucket,
		Key:    &key,
	}

	_, err := s.awsS3.PutObject(poi)
	return err
}

func (s *s3Client) getObject(bucket string, key string) (string, error) {
	goi := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	goo, err := s.awsS3.GetObject(goi)
	if err != nil {
		return "", err
	}

	payload, err := ioutil.ReadAll(goo.Body)
	return string(payload), err
}
