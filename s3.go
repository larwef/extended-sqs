package kitsune

import (
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type s3Client struct {
	opts  *options
	awsS3 s3iface.S3API
}
