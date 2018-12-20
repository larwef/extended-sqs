package kitsune

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"math"
	"strconv"
)

const (
	// AttributeNameS3Bucket is an attribute name used by sender to pass the location of messages put on S3 to the receiver.
	// Receiver uses this attribute if set to fetch a message from S3.
	AttributeNameS3Bucket = "payloadBucket"

	// AttributeNameKMSKey used to pass the KMS key used to encrypt the data key.
	AttributeNameKMSKey = "kmsKey"
)

// Client object handles communication with SQS
type Client struct {
	opts options

	awsConfig aws.Config

	awsSQSClient *sqsClient
	awsS3Client  *s3Client
	awsKMSClient *kmsClient
}

type options struct {
	delaySeconds             int64
	maxNumberOfMessages      int64
	initialVisibilityTimeout int64
	maxVisibilityTimeout     int64
	backoffFactor            int64
	backoffFunction          func(int64, int64, int64, int64) int64
	waitTimeSeconds          int64
	attributeNames           []*string
	messageAttributeNames    []*string
	s3Bucket                 string
	forceS3                  bool
	kmsKeyID                 string
}

var defaultClientOptions = options{
	delaySeconds:             0,
	maxNumberOfMessages:      10,
	initialVisibilityTimeout: 60,
	backoffFactor:            2,
	maxVisibilityTimeout:     900,
	waitTimeSeconds:          20,
	attributeNames:           []*string{aws.String("ApproximateReceiveCount")},
	messageAttributeNames:    []*string{aws.String(AttributeNameS3Bucket), aws.String(AttributeNameKMSKey)},
	forceS3:                  false,
}

// ClientOption sets configuration options for a awsSQSClient.
type ClientOption func(*options)

// DelaySeconds is used to set the DelaySeconds property on the awsSQSClient which is how many seconds the message will be
// unavaible once its put on a queue.
func DelaySeconds(d int64) ClientOption {
	return func(o *options) { o.delaySeconds = d }
}

// MaxNumberOfMessages sets the maximum number of messages can be returned each time the awsSQSClient fetches messages.
func MaxNumberOfMessages(m int64) ClientOption {
	return func(o *options) { o.maxNumberOfMessages = m }
}

// InitialVisibilityTimeout sets the initial time used when changing message visibility. The length of subsequent changes will be
// determined by strategy defined by the backoff function used.
func InitialVisibilityTimeout(i int64) ClientOption {
	return func(o *options) { o.initialVisibilityTimeout = i }
}

// MaxVisibilityTimeout sets the maxiumum time a message can be made unavailable by chaning message visibility.
func MaxVisibilityTimeout(m int64) ClientOption {
	return func(o *options) { o.maxVisibilityTimeout = m }
}

// BackoffFactor sets the backoff factor which is a paramter used by the backoff function.
func BackoffFactor(b int64) ClientOption {
	return func(o *options) { o.backoffFactor = b }
}

// BackoffFunction sets the function which computes the next visibility timeout.
func BackoffFunction(f func(int64, int64, int64, int64) int64) ClientOption {
	return func(o *options) { o.backoffFunction = f }
}

// WaitTimeSeconds sets the time a client will wait for messages on each call.
func WaitTimeSeconds(w int64) ClientOption {
	return func(o *options) { o.waitTimeSeconds = w }
}

// AttributeNames sets the attributes to be returned when fetching messages. ApproximateReceiveCount is always returned because it
// is used when calculating backoff.
func AttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for i := range s {
			o.attributeNames = append(o.attributeNames, &s[i])
		}
	}
}

// MessageAttributeNames sets the message attributes to be returned when fetching messages. ApproximateReceiveCount is always
// returned because it is used when calculating backoff.
func MessageAttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for i := range s {
			o.messageAttributeNames = append(o.messageAttributeNames, &s[i])
		}
	}
}

// S3Bucket sets the bucket where the client should put messages which exceed max size.
func S3Bucket(s string) ClientOption {
	return func(o *options) { o.s3Bucket = s }
}

// ForceS3 set if all messages should be saved to S3.
func ForceS3(b bool) ClientOption {
	return func(o *options) { o.forceS3 = b }
}

// KMSKeyID sets the KMS key to be used for encryption
func KMSKeyID(s string) ClientOption {
	return func(o *options) { o.kmsKeyID = s }
}

// New returns a new awsSQSClient with configuration set as defined by the ClientOptions. Will create a s3Client from the
// aws.Config if a bucket is set.
func New(awsConfig *aws.Config, opt ...ClientOption) (*Client, error) {
	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("Error getting AWS awsSession: %v ", err)
	}

	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	sqsc := &sqsClient{
		opts:   &opts,
		awsSQS: sqs.New(awsSession),
	}

	var s3c *s3Client
	if opts.s3Bucket != "" {
		s3c = &s3Client{
			awsS3: s3.New(awsSession),
		}
	}

	var kmsc *kmsClient
	if opts.kmsKeyID != "" {
		kmsc = &kmsClient{
			opts:   &opts,
			awsKMS: kms.New(awsSession),
		}
	}

	return &Client{
		opts:         opts,
		awsSQSClient: sqsc,
		awsS3Client:  s3c,
		awsKMSClient: kmsc,
	}, nil
}

// SendMessage sends a message to the specified queue. Convenient method for sending a message without custom attributes. This
// does not guarantee there will be no attributes on the message to SQS. The client might add attributes eg. for file events when
// the payload is uploaded to S3.
func (c *Client) SendMessage(queueName string, payload string) error {
	return c.SendMessageWithAttributes(queueName, payload, nil)
}

// SendMessageWithAttributes sends a message to the specified queue with attributes. If the message size exceeds maximum, the
// payload will be uploaded to the configured S3 bucket and a file event will be sent on the SQS queue. The bucket where the
// message was uploaded if put on the message attributes. This means an no of attributes error can be thrown even though this
// function is called with less than maximum number of attributes.
func (c *Client) SendMessageWithAttributes(queueName string, payload string, messageAttributes map[string]*sqs.MessageAttributeValue) error {
	// Encrypt the payload if a KMS key is configured
	if c.opts.kmsKeyID != "" {
		var err error
		encryptedPayload, err := c.awsKMSClient.Encrypt([]byte(payload))
		if err != nil {
			return err
		}

		if messageAttributes == nil {
			messageAttributes = make(map[string]*sqs.MessageAttributeValue)
		}
		messageAttributes[AttributeNameKMSKey] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: &c.opts.kmsKeyID}
		payload = string(encryptedPayload)
	}

	// Put payload to S3 if S3 is forced of message is larger than max size. Bucket needs to be configured.
	if (c.opts.forceS3 || getMessageSize(payload, messageAttributes) > maxMessageSize) && c.opts.s3Bucket != "" {
		key := uuid.New().String()
		if err := c.awsS3Client.putObject(c.opts.s3Bucket, key, payload); err != nil {
			return fmt.Errorf("error puting object to S3: %v", err)
		}

		fe := fileEvent{
			Size:     int64(len(payload)),
			Filename: key,
		}

		bytes, err := json.Marshal(&fe)
		if err != nil {
			return err
		}

		if messageAttributes == nil {
			messageAttributes = make(map[string]*sqs.MessageAttributeValue)
		}
		messageAttributes[AttributeNameS3Bucket] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: &c.opts.s3Bucket}
		payload = string(bytes)
	}

	return c.awsSQSClient.sendMessage(queueName, payload, messageAttributes)
}

// ReceiveMessage polls the specified queue and returns the fetched messages. If the S3 bucket attribute is set, the payload is
// fetched and replaces the file event in the sqs.Message body. This will not delete the object in S3. A lifecycle rule is
// recomended.
func (c *Client) ReceiveMessage(queueName string) ([]*sqs.Message, error) {
	messages, err := c.awsSQSClient.receiveMessage(queueName)

	for _, message := range messages {
		// If S3 bucket is included the payload is located in S3 an needs to be fetched
		if val, exists := message.MessageAttributes[AttributeNameS3Bucket]; exists {
			var fe fileEvent
			if err := json.Unmarshal([]byte(*message.Body), &fe); err != nil {
				return nil, err
			}

			if payload, err := c.awsS3Client.getObject(*val.StringValue, fe.Filename); err == nil {
				message.Body = &payload
				delete(message.MessageAttributes, AttributeNameS3Bucket)
			} else {
				return nil, err
			}
		}

		// If KMS key is included the payload is encrypted and needs to be decrypted
		if _, exists := message.MessageAttributes[AttributeNameKMSKey]; exists {
			decrypted, err := c.awsKMSClient.Decrypt([]byte(*message.Body))
			if err != nil {
				return nil, err
			}
			decryptedStr := string(decrypted)
			message.Body = &decryptedStr
			delete(message.MessageAttributes, AttributeNameKMSKey)
		}
	}

	return messages, err
}

// ChangeMessageVisibility changes the visibilty of a message. Essentialy putting it back in the queue and unavailable for a
// specified amount of time.
func (c *Client) ChangeMessageVisibility(queueName string, message *sqs.Message, timeout int64) error {
	return c.awsSQSClient.changeMessageVisibility(queueName, message, timeout)
}

// Backoff is used for changing message visibility based on a calculated amount of time determined by a back off function
// configured on the awsSQSClient.
func (c *Client) Backoff(queueName string, message *sqs.Message) error {
	receivedCount, err := strconv.Atoi(*message.Attributes["ApproximateReceiveCount"])
	if err != nil {
		return errors.New("error getting received count")
	}

	receivedCount64 := int64(receivedCount)

	timeout := c.opts.backoffFunction(receivedCount64, c.opts.initialVisibilityTimeout, c.opts.maxVisibilityTimeout, c.opts.backoffFactor)
	return c.awsSQSClient.changeMessageVisibility(queueName, message, timeout)
}

// DeleteMessage removes a message from the queue.
func (c *Client) DeleteMessage(queueName string, receiptHandle *string) error {
	return c.awsSQSClient.deleteMessage(queueName, receiptHandle)
}

// ExponentialBackoff can be configured on a client to achieve an exponential backoff strategy based on how many times the message
// is received.
func ExponentialBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	receiveCount := min(retryCount, 9999)
	retryNumber := max(receiveCount-1, 0)
	expTimeout := int64(math.Pow(float64(backoffFactor), float64(retryNumber)) * float64(minBackoff))

	return min(expTimeout, maxBackof)
}

// LinearBackoff can be configured on a awsSQSClient to achieve a linear backoff strategy based on how many times a message is
// received.
func LinearBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	return min(minBackoff+(retryCount-1)*backoffFactor, maxBackof)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}
