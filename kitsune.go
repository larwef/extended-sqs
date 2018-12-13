package kitsune

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"math"
	"strconv"
)

// Client object handles communication with SQS
type Client struct {
	opts options

	awsConfig aws.Config

	awsSQSClient *sqsClient
}

type options struct {
	awsS3   s3iface.S3API
	forceS3 bool

	delaySeconds             int64
	maxNumberOfMessages      int64
	initialVisibilityTimeout int64
	maxVisibilityTimeout     int64
	backoffFactor            int64
	backoffFunction          func(int64, int64, int64, int64) int64
	waitTimeSeconds          int64
	attributeNames           []*string
	messageAttributeNames    []*string
}

var defaultClientOptions = options{
	awsS3:   nil,
	forceS3: false,

	delaySeconds:             0,
	maxNumberOfMessages:      10,
	initialVisibilityTimeout: 60,
	backoffFactor:            2,
	maxVisibilityTimeout:     900,
	waitTimeSeconds:          20,
	attributeNames:           []*string{aws.String("ApproximateReceiveCount")},
}

// ClientOption sets configuration options for a awsSQSClient.
type ClientOption func(*options)

// AwsS3 is used to set an S3 client to store large messages (or all messages if forceS3 is set to true). There is no cleanup or
// deleting of messages. It is recomended to set a lifecycle policy on the bucket used. Remeber to let the messages be in the
// bucket for at least as long as the retention period on the queue to avoid dropping messages.
func AwsS3(i s3iface.S3API) ClientOption {
	return func(o *options) { o.awsS3 = i }
}

// ForceS3 set if all messages should be saved to S3
func ForceS3(b bool) ClientOption {
	return func(o *options) { o.forceS3 = b }
}

// DelaySeconds is used to set the DelaySeconds property on the awsSQSClient which is how many seconds the message will be unavaible
// once its put on a queue.
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

// MessageAttributeNames sets the message attributes to be returned when fetching messages. ApproximateReceiveCount is always returned
// because it is used when calculating backoff.
func MessageAttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for i := range s {
			o.messageAttributeNames = append(o.messageAttributeNames, &s[i])
		}
	}
}

// NewClient returns a new awsSQSClient with configuration set as defined by the ClientOptions.
func NewClient(awsConfig *aws.Config, opt ...ClientOption) (*Client, error) {
	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("Error getting AWS awsSession: %v ", err)
	}

	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	return &Client{
		opts: opts,
		awsSQSClient: &sqsClient{
			awsSQS: sqs.New(awsSession),
			opts:   &opts,
		},
	}, nil
}

// SendMessage sends a message to the specified queue.
func (c *Client) SendMessage(queueName string, payload string) error {
	return c.awsSQSClient.sendMessage(queueName, payload, nil)
}

// SendMessageWithAttributes sends a message to the specified queue with attributes.
func (c *Client) SendMessageWithAttributes(queueName string, payload string, attributes map[string]*sqs.MessageAttributeValue) error {
	return c.awsSQSClient.sendMessage(queueName, payload, attributes)
}

// ReceiveMessage polls the specified queue and returns the fetched messages.
func (c *Client) ReceiveMessage(queueName string) ([]*sqs.Message, error) {
	return c.awsSQSClient.receiveMessage(queueName)
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

// ExponentialBackoff can be configured on a client to achieve an exponential backoff strategy based on how many times the
// message is received.
func ExponentialBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	receiveCount := min(retryCount, 9999)
	retryNumber := max(receiveCount-1, 0)
	expTimeout := int64(math.Pow(float64(backoffFactor), float64(retryNumber)) * float64(minBackoff))

	return min(expTimeout, maxBackof)
}

// LinearBackoff can be configured on a awsSQSClient to achieve a linear backoff strategy based on how many times a message is received.
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
