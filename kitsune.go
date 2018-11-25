package kitsune

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
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
	waitTimeSeconds          int64
}

var defaultClientOptions = options{
	delaySeconds:             0,
	maxNumberOfMessages:      10,
	initialVisibilityTimeout: 60,
	maxVisibilityTimeout:     900,
	waitTimeSeconds:          20,
}

type ClientOption func(*options)

func DelaySeconds(d int64) ClientOption             { return func(o *options) { o.delaySeconds = d } }
func MaxNumberOfMessages(m int64) ClientOption      { return func(o *options) { o.maxNumberOfMessages = m } }
func InitialVisibilityTimeout(i int64) ClientOption { return func(o *options) { o.initialVisibilityTimeout = i } }
func MaxVisibilityTimeout(m int64) ClientOption     { return func(o *options) { o.maxNumberOfMessages = m } }
func WaitTimeSeconds(w int64) ClientOption          { return func(o *options) { o.waitTimeSeconds = w } }

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
		MaxNumberOfMessages: &c.opts.maxNumberOfMessages,
		QueueUrl:            queueURL,
		VisibilityTimeout:   &c.opts.initialVisibilityTimeout,
		WaitTimeSeconds:     &c.opts.waitTimeSeconds,
	}

	output, err := c.awsSqs.ReceiveMessage(rmi)
	return output.Messages, err
}

func (c *Client) ChangeMessageVisibility(queueName *string, message *sqs.Message) error {
	queueURL, err := c.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	cmvi := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueURL,
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: c.getNextVisibilityTimeoutSeconds(message),
	}

	_, err = c.awsSqs.ChangeMessageVisibility(cmvi)
	return err
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

// TODO: Implement backoff function
func (c *Client) getNextVisibilityTimeoutSeconds(message *sqs.Message) *int64 {
	return &c.opts.initialVisibilityTimeout
}

func (c *Client) getQueueUrl(queueName *string) (*string, error) {
	output, err := c.awsSqs.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	return output.QueueUrl, err
}
