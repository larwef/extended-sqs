package kitsune

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"math"
	"strconv"
)

const (
	// The maximum size of a SQS payload is 262,144 bytes
	maxMessageSize = 256 * 1024
	// The maximum number of custom attributes are 10
	maxNumberOfAttributes = 10
)

// ErrorMaxMessageSizeExceeded is returned when the combined size of the payload and the message attributes exceeds maxMessageSize.
var ErrorMaxMessageSizeExceeded = fmt.Errorf("maximum message size of %d bytes exceeded", maxMessageSize)

// ErrorMaxNumberOfAttributesExceeded is returned when the number of attributes exceeds maxNumberOfAttributes.
var ErrorMaxNumberOfAttributesExceeded = fmt.Errorf("maximum number of attributes of %d exceeded", maxNumberOfAttributes)

// Client object handles communication with SQS
type Client struct {
	opts options

	awsSqs sqsiface.SQSAPI
}

type options struct {
	awsS3                    s3iface.S3API
	forceS3                  bool
	delaySeconds             int64
	maxNumberOfMessages      int64
	initialVisibilityTimeout int64
	maxVisibilityTimeout     int64
	backoffFactor            int64
	backoffFunction          func(int64, int64, int64, int64) int64
	waitTimeSeconds          int64
	attributeNames           []*string
}

var defaultClientOptions = options{
	awsS3:                    nil,
	forceS3:                  false,
	delaySeconds:             0,
	maxNumberOfMessages:      10,
	initialVisibilityTimeout: 60,
	backoffFactor:            2,
	maxVisibilityTimeout:     900,
	waitTimeSeconds:          20,
	attributeNames:           []*string{aws.String("ApproximateReceiveCount")},
}

// ClientOption sets configuration options for a Client.
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

// DelaySeconds is used to set the DelaySeconds property on the Client which is how many seconds the message will be unavaible
// once its put on a queue.
func DelaySeconds(d int64) ClientOption {
	return func(o *options) { o.delaySeconds = d }
}

// MaxNumberOfMessages sets the maximum number of messages can be returned each time the Client fetches messages.
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

// AttributeNames sets the message attributes to be returned when fetching messages. ApproximateReceiveCount is always returned
// because it is used when calculating backoff.
func AttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for _, str := range s {
			o.attributeNames = append(o.attributeNames, &str)
		}
	}
}

// NewClient returns a new Client with configuration set as defined by the ClientOptions.
func NewClient(sqs sqsiface.SQSAPI, opt ...ClientOption) *Client {
	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	return &Client{
		opts:   opts,
		awsSqs: sqs,
	}
}

// SendMessage sends a message to the specified queue.
func (c *Client) SendMessage(queueName string, payload string) error {
	return c.sendMessage(queueName, payload, nil)
}

// SendMessageWithAttributes sends a message to the specified queue with attributes.
func (c *Client) SendMessageWithAttributes(queueName string, payload string, attributes map[string]*sqs.MessageAttributeValue) error {
	return c.sendMessage(queueName, payload, attributes)
}

func (c *Client) sendMessage(queueName string, payload string, attributes map[string]*sqs.MessageAttributeValue) error {
	return c.sendToSQS(queueName, payload, attributes)
}

func (c *Client) sendToSQS(queueName string, payload string, attributes map[string]*sqs.MessageAttributeValue) error {
	if len(attributes) > maxNumberOfAttributes {
		return ErrorMaxNumberOfAttributesExceeded
	}

	if getMessageSize(payload, attributes) > maxMessageSize {
		return ErrorMaxMessageSizeExceeded
	}

	queueURL, err := c.getQueueURL(&queueName)
	if err != nil {
		return err
	}

	smi := &sqs.SendMessageInput{
		DelaySeconds:      &c.opts.delaySeconds,
		MessageAttributes: attributes,
		MessageBody:       &payload,
		QueueUrl:          queueURL,
	}

	_, err = c.awsSqs.SendMessage(smi)
	return err
}

// ReceiveMessage polls the specified queue and returns the fetched messages.
func (c *Client) ReceiveMessage(queueName string) ([]*sqs.Message, error) {
	queueURL, err := c.getQueueURL(&queueName)
	if err != nil {
		return nil, err
	}

	rmi := &sqs.ReceiveMessageInput{
		AttributeNames:      c.opts.attributeNames,
		MaxNumberOfMessages: &c.opts.maxNumberOfMessages,
		QueueUrl:            queueURL,
		VisibilityTimeout:   &c.opts.initialVisibilityTimeout,
		WaitTimeSeconds:     &c.opts.waitTimeSeconds,
	}

	output, err := c.awsSqs.ReceiveMessage(rmi)
	return output.Messages, err
}

// ChangeMessageVisibility changes the visibilty of a message. Essentialy putting it back in the queue and unavailable for a
// specified amount of time.
func (c *Client) ChangeMessageVisibility(queueName string, message *sqs.Message, timeout int64) error {
	queueURL, err := c.getQueueURL(&queueName)
	if err != nil {
		return err
	}

	cmvi := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueURL,
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: &timeout,
	}

	_, err = c.awsSqs.ChangeMessageVisibility(cmvi)
	return err
}

// Backoff is used for changing message visibility based on a calculated amount of time determined by a back off function
// configured on the Client.
func (c *Client) Backoff(queueName string, message *sqs.Message) error {
	receivedCount, err := strconv.Atoi(*message.Attributes["ApproximateReceiveCount"])
	if err != nil {
		return errors.New("error getting received count")
	}

	receivedCount64 := int64(receivedCount)

	timeout := c.opts.backoffFunction(receivedCount64, c.opts.initialVisibilityTimeout, c.opts.maxVisibilityTimeout, c.opts.backoffFactor)
	return c.ChangeMessageVisibility(queueName, message, timeout)
}

// DeleteMessage removes a message from the queue.
func (c *Client) DeleteMessage(queueName string, receiptHandle *string) error {
	queueURL, err := c.getQueueURL(&queueName)
	if err != nil {
		return err
	}

	dmi := &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: receiptHandle,
	}

	_, err = c.awsSqs.DeleteMessage(dmi)
	return err
}

func (c *Client) getQueueURL(queueName *string) (*string, error) {
	output, err := c.awsSqs.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	return output.QueueUrl, err
}

func getMessageSize(payload string, attributes map[string]*sqs.MessageAttributeValue) int {
	size := len(payload)

	for key, value := range attributes {
		size += len(key) + len(*value.DataType) + len(*value.StringValue)
	}

	return size
}

// ExponentialBackoff can be configured on a client to achieve an exponential backoff strategy based on how many times the
// message is received.
func ExponentialBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	receiveCount := min(retryCount, 9999)
	retryNumber := max(receiveCount-1, 0)
	expTimeout := int64(math.Pow(float64(backoffFactor), float64(retryNumber)) * float64(minBackoff))

	return min(expTimeout, maxBackof)
}

// LinearBackoff can be configured on a Client to achieve a linear backoff strategy based on how many times a message is received.
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
