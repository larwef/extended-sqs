package kitsune

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"math"
	"strconv"
)

type Client struct {
	opts options

	awsSqs sqsiface.SQSAPI
}

type options struct {
	delaySeconds             int64
	maxNumberOfMessages      int64
	initialVisibilityTimeout int64
	maxVisibilityTimeout     int64
	backoffFactor            int64
	backoffFunction          func(int64, int64, int64, int64) *int64
	waitTimeSeconds          int64
	attributeNames           []*string
}

var defaultClientOptions = options{
	delaySeconds:             0,
	maxNumberOfMessages:      10,
	initialVisibilityTimeout: 60,
	backoffFactor:            2,
	maxVisibilityTimeout:     900,
	waitTimeSeconds:          20,
	attributeNames:           []*string{aws.String("ApproximateReceiveCount")},
}

type ClientOption func(*options)

func DelaySeconds(d int64) ClientOption                                      { return func(o *options) { o.delaySeconds = d } }
func MaxNumberOfMessages(m int64) ClientOption                               { return func(o *options) { o.maxNumberOfMessages = m } }
func InitialVisibilityTimeout(i int64) ClientOption                          { return func(o *options) { o.initialVisibilityTimeout = i } }
func MaxVisibilityTimeout(m int64) ClientOption                              { return func(o *options) { o.maxVisibilityTimeout = m } }
func BackoffFactor(b int64) ClientOption                                     { return func(o *options) { o.backoffFactor = b } }
func BackoffFunction(f func(int64, int64, int64, int64) *int64) ClientOption { return func(o *options) { o.backoffFunction = f } }
func WaitTimeSeconds(w int64) ClientOption                                   { return func(o *options) { o.waitTimeSeconds = w } }
func AttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for _, str := range s {
			o.attributeNames = append(o.attributeNames, &str)
		}
	}
}

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

func (c *Client) SendMessage(queueName *string, payload string) error {
	queueURL, err := c.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	smi := &sqs.SendMessageInput{
		DelaySeconds: &c.opts.delaySeconds,
		MessageBody:  &payload,
		QueueUrl:     queueURL,
	}

	_, err = c.awsSqs.SendMessage(smi)
	return err
}

func (c *Client) SendMessageWithAttributes(queueName *string, payload *string, attributes map[string]*sqs.MessageAttributeValue) error {
	queueURL, err := c.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	smi := &sqs.SendMessageInput{
		DelaySeconds:      &c.opts.delaySeconds,
		MessageAttributes: attributes,
		MessageBody:       payload,
		QueueUrl:          queueURL,
	}

	_, err = c.awsSqs.SendMessage(smi)
	return err
}

func (c *Client) ReceiveMessage(queueName *string) ([]*sqs.Message, error) {
	queueURL, err := c.getQueueUrl(queueName)
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

func (c *Client) ChangeMessageVisibility(queueName *string, message *sqs.Message, timeout *int64) error {
	queueURL, err := c.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	cmvi := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueURL,
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: timeout,
	}

	_, err = c.awsSqs.ChangeMessageVisibility(cmvi)
	return err
}

func (c *Client) Backoff(queueName *string, message *sqs.Message) error {
	receivedCount, err := strconv.Atoi(*message.Attributes["ApproximateReceiveCount"])
	if err != nil {
		return errors.New("error getting received count")
	}

	receivedCount64 := int64(receivedCount)

	timeout := c.opts.backoffFunction(receivedCount64, c.opts.initialVisibilityTimeout, c.opts.maxVisibilityTimeout, c.opts.backoffFactor)
	return c.ChangeMessageVisibility(queueName, message, timeout)
}

func (c *Client) DeleteMessage(queueName *string, message *sqs.Message) error {
	queueURL, err := c.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	dmi := &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: message.ReceiptHandle,
	}

	_, err = c.awsSqs.DeleteMessage(dmi)
	return err
}

func (c *Client) getQueueUrl(queueName *string) (*string, error) {
	output, err := c.awsSqs.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	return output.QueueUrl, err
}

func ExponentialBackoff(retryCount int64, minBackoff int64, maxBackof int64, backoffFactor int64) *int64 {
	receiveCount := min(retryCount, 9999)
	retryNumber := max(receiveCount-1, 0)

	expTimeout := int64(math.Pow(float64(backoffFactor), float64(retryNumber))*float64(minBackoff))

	timeout := min(expTimeout, maxBackof)
	return &timeout
}

func LinearBackoff(retryCount int64, minBackoff int64, maxBackof int64, backoffFactor int64) *int64 {
	i := min(minBackoff+(retryCount-1)*backoffFactor, maxBackof)
	return &i
}

func min(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
