package kitsune

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"sync"
)

const (
	// The maximum size of a SQS payload is 262,144 bytes.
	maxMessageSize = 256 * 1024
	// The maximum number of custom attributes are 10.
	maxNumberOfAttributes = 10
)

// ErrorMaxMessageSizeExceeded is returned when the combined size of the payload and the message attributes exceeds maxMessageSize.
var ErrorMaxMessageSizeExceeded = fmt.Errorf("maximum message size of %d bytes exceeded", maxMessageSize)

// ErrorMaxNumberOfAttributesExceeded is returned when the number of attributes exceeds maxNumberOfAttributes.
var ErrorMaxNumberOfAttributesExceeded = fmt.Errorf("maximum number of attributes of %d exceeded", maxNumberOfAttributes)

func size(payload []byte, messageAttributes map[string]*sqs.MessageAttributeValue) int {
	size := len(payload)

	for key, value := range messageAttributes {
		size += len(key) + len(*value.DataType) + len(*value.StringValue)
	}

	return size
}

type sqsClient struct {
	opts       *options
	queueCache map[string]string
	awsSQS     sqsiface.SQSAPI
	rwLock     sync.RWMutex
}

func newSQSClient(awsSQS sqsiface.SQSAPI, opts *options) *sqsClient {
	return &sqsClient{
		opts:       opts,
		queueCache: make(map[string]string),
		awsSQS:     awsSQS,
	}
}

func (s *sqsClient) sendMessage(queueName *string, payload []byte, messageAttributes map[string]*sqs.MessageAttributeValue) error {
	if len(messageAttributes) > maxNumberOfAttributes {
		return ErrorMaxNumberOfAttributesExceeded
	}

	if size(payload, messageAttributes) > maxMessageSize {
		return ErrorMaxMessageSizeExceeded
	}

	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return err
	}

	smi := &sqs.SendMessageInput{
		DelaySeconds:      &s.opts.delaySeconds,
		MessageAttributes: messageAttributes,
		MessageBody:       aws.String(string(payload)),
		QueueUrl:          queueURL,
	}

	_, err = s.awsSQS.SendMessage(smi)
	return err
}

func (s *sqsClient) sendMessageBatch(queueName *string, entries []*sqs.SendMessageBatchRequestEntry) (*sqs.SendMessageBatchOutput, error) {
	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return nil, err
	}

	sbo := &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: queueURL,
	}

	return s.awsSQS.SendMessageBatch(sbo)
}

func (s *sqsClient) receiveMessage(queueName *string) ([]*sqs.Message, error) {
	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return nil, err
	}

	rmi := &sqs.ReceiveMessageInput{
		AttributeNames:        s.opts.attributeNames,
		MaxNumberOfMessages:   &s.opts.maxNumberOfMessages,
		MessageAttributeNames: s.opts.messageAttributeNames,
		QueueUrl:              queueURL,
		VisibilityTimeout:     &s.opts.initialVisibilityTimeout,
		WaitTimeSeconds:       &s.opts.waitTimeSeconds,
	}

	output, err := s.awsSQS.ReceiveMessage(rmi)
	return output.Messages, err
}

func (s *sqsClient) changeMessageVisibility(queueName *string, message *sqs.Message, timeout int64) error {
	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return err
	}

	cmvi := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueURL,
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: &timeout,
	}

	_, err = s.awsSQS.ChangeMessageVisibility(cmvi)
	return err
}

func (s *sqsClient) deleteMessage(queueName *string, receiptHandle *string) error {
	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return err
	}

	dmi := &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: receiptHandle,
	}

	_, err = s.awsSQS.DeleteMessage(dmi)
	return err
}

func (s *sqsClient) getQueueURL(queueName *string) (*string, error) {
	s.rwLock.RLock()
	if value, exists := s.queueCache[*queueName]; exists {
		s.rwLock.RUnlock()
		return &value, nil
	}
	s.rwLock.RUnlock()

	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	output, err := s.awsSQS.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	if err == nil {
		s.queueCache[*queueName] = *output.QueueUrl
	}

	return output.QueueUrl, err
}
