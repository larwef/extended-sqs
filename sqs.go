package kitsune

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
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

type sqsClient struct {
	opts   *options
	awsSQS sqsiface.SQSAPI
}

func (s *sqsClient) sendMessage(queueName *string, payload *string, attributes map[string]*sqs.MessageAttributeValue) error {
	if len(attributes) > maxNumberOfAttributes {
		return ErrorMaxNumberOfAttributesExceeded
	}

	if getMessageSize(*payload, attributes) > maxMessageSize {
		return ErrorMaxMessageSizeExceeded
	}

	queueURL, err := s.getQueueURL(queueName)
	if err != nil {
		return err
	}

	smi := &sqs.SendMessageInput{
		DelaySeconds:      &s.opts.delaySeconds,
		MessageAttributes: attributes,
		MessageBody:       payload,
		QueueUrl:          queueURL,
	}

	_, err = s.awsSQS.SendMessage(smi)
	return err
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
	output, err := s.awsSQS.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: queueName})
	return output.QueueUrl, err
}

func getMessageSize(payload string, attributes map[string]*sqs.MessageAttributeValue) int {
	size := len(payload)

	for key, value := range attributes {
		size += len(key) + len(*value.DataType) + len(*value.StringValue)
	}

	return size
}
