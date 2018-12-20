package test

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"time"
)

// SQSMock is used to mock SQS during testing. Does not behave exactly as SQS, but should be sufficient for testing and provides
// a few useful functions when testing. Such as waiting for messages to be deleted, waiting for received messages etc.
type SQSMock struct {
	sqsiface.SQSAPI

	timeoutSec     int64
	chanBufferSize int64

	sendMessageRequests             map[string]chan *sqs.SendMessageInput
	changeMessageVisibilityRequests map[string]chan *sqs.ChangeMessageVisibilityInput
	deleteMessageRequests           map[string]chan *sqs.DeleteMessageInput
}

// NewSQSMock returns a new SQSMock.
func NewSQSMock(timeoutSec int64, chanBufferSize int64) *SQSMock {
	return &SQSMock{
		timeoutSec:     timeoutSec,
		chanBufferSize: chanBufferSize,

		sendMessageRequests:             make(map[string]chan *sqs.SendMessageInput),
		changeMessageVisibilityRequests: make(map[string]chan *sqs.ChangeMessageVisibilityInput),
		deleteMessageRequests:           make(map[string]chan *sqs.DeleteMessageInput),
	}
}

// SendMessage sends a message to the mock.
func (sm *SQSMock) SendMessage(smi *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	if c, exists := sm.sendMessageRequests[*smi.QueueUrl]; exists {
		c <- smi
		return &sqs.SendMessageOutput{}, nil
	}

	return nil, errors.New("queue doesnt exist")
}

// ReceiveMessage receives a message from the mock.
func (sm *SQSMock) ReceiveMessage(rmi *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	rmo := &sqs.ReceiveMessageOutput{}

	c, exists := sm.sendMessageRequests[*rmi.QueueUrl]
	if !exists {
		return nil, errors.New("queue doesnt exist")
	}

	for {
		select {
		case messageInput := <-c:
			rmo.Messages = append(rmo.Messages, &sqs.Message{
				Body:              messageInput.MessageBody,
				MessageAttributes: messageInput.MessageAttributes,
			})

			if len(rmo.Messages) == int(*rmi.MaxNumberOfMessages) {
				return rmo, nil
			}
		default:
			return rmo, nil
		}
	}
}

// ChangeMessageVisibility sends a request to change a messages visibility to the mock.
func (sm *SQSMock) ChangeMessageVisibility(cmvi *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	if c, exists := sm.changeMessageVisibilityRequests[*cmvi.QueueUrl]; exists {
		c <- cmvi
		return &sqs.ChangeMessageVisibilityOutput{}, nil
	}

	return nil, errors.New("queue doesnt exist")
}

// DeleteMessage sends a delete message request to the mock.
func (sm *SQSMock) DeleteMessage(dmi *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	if c, exists := sm.deleteMessageRequests[*dmi.QueueUrl]; exists {
		c <- dmi
		return &sqs.DeleteMessageOutput{}, nil
	}

	return nil, errors.New("queue doesnt exist")
}

// CreateQueueIfNotExists will create a queue representation on the mock if one with the same name doesnt already exist.
func (sm *SQSMock) CreateQueueIfNotExists(queueURL *string) {
	if _, exists := sm.sendMessageRequests[*queueURL]; !exists {
		sm.sendMessageRequests[*queueURL] = make(chan *sqs.SendMessageInput, sm.chanBufferSize)
	}

	if _, exists := sm.sendMessageRequests[*queueURL]; !exists {
		sm.changeMessageVisibilityRequests[*queueURL] = make(chan *sqs.ChangeMessageVisibilityInput, sm.chanBufferSize)
	}

	if _, exists := sm.sendMessageRequests[*queueURL]; !exists {
		sm.deleteMessageRequests[*queueURL] = make(chan *sqs.DeleteMessageInput, sm.chanBufferSize)
	}
}

// GetQueueUrl is primarily implemented so the client can resolve the queues in the mock.
// Ignore lint output for this function. Cant rename it because its part of an interface which is defined elsewhere.
func (sm *SQSMock) GetQueueUrl(gqui *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: gqui.QueueName,
	}, nil
}

// WaitUntilMessageDeleted will wait until count delete message requests are received by the mock. Will time out after a configurable amount of time and
// return an error.
func (sm *SQSMock) WaitUntilMessageDeleted(queueURL *string, count int) error {
	messagesDeleted := 0
	c, exists := sm.deleteMessageRequests[*queueURL]
	if !exists {
		return errors.New("queue doesnt exist")
	}

	for {
		select {
		case <-c:
			messagesDeleted++
			if messagesDeleted == count {
				return nil
			}
		case <-time.After(time.Duration(sm.timeoutSec) * time.Second):
			return errors.New("timed out waiting for message to be deleted")
		}
	}
}

// WaitUntilMessagesReceived waits until count messages are received and returns the received messages. Will time out after a
// configurable amount of time and return an error.
func (sm *SQSMock) WaitUntilMessagesReceived(queueURL *string, count int) ([]*sqs.Message, error) {
	var messages []*sqs.Message
	noOfMessagesReceived := 0

	receiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(int64(count + 1)),
	}

	msges, err := sm.ReceiveMessage(receiveMessageInput)
	if err != nil {
		return nil, err
	}

	messages = append(messages, msges.Messages...)
	noOfMessagesReceived = noOfMessagesReceived + len(msges.Messages)

	if noOfMessagesReceived > count {
		return nil, errors.New("received more messages than expected")
	} else if noOfMessagesReceived < count {
		return nil, errors.New("received less messages than expected")
	}

	return messages, nil
}
