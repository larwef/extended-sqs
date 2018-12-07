package kitsune

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/larwef/kitsune/test"
	"strconv"
	"testing"
)

// Currently this is testing the mock just as much as the Client, but since the mock is made to be used by other packages needing
// to mock the client for testing these tests might be useful. See the integration tests for more proper tests of the Client.

func TestClient_SendMessage(t *testing.T) {
	for i := 1; i <= 100; i++ {
		sendNMessagesTest(t, i)
	}
}

func TestClient_ReceiveMessage(t *testing.T) {
	for i := 1; i <= 100; i++ {
		receiveNMessagesTest(t, i)
	}
}

func sendNMessagesTest(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := NewClient(sqsMock)

	sqsMock.CreateQueueIfNotExists("test-queue")

	for i := 0; i < n; i++ {
		err := sqsClient.SendMessage("test-queue", "Testpayload"+strconv.Itoa(i))
		test.AssertNotError(t, err)
	}

	messages, err := sqsMock.WaitUntilMessagesReceived("test-queue", n)
	test.AssertNotError(t, err)

	for i := 0; i < n; i++ {
		test.AssertEqual(t, *messages[i].Body, "Testpayload"+strconv.Itoa(i))
	}
}

func receiveNMessagesTest(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := NewClient(sqsMock)

	sqsMock.CreateQueueIfNotExists("test-queue")

	for i := 0; i < n; i++ {
		sendMessageInput := &sqs.SendMessageInput{
			MessageBody: aws.String("Testpayload" + strconv.Itoa(i)),
			QueueUrl:    aws.String("test-queue"),
		}
		_, err := sqsMock.SendMessage(sendMessageInput)
		test.AssertNotError(t, err)
	}

	var messages []*sqs.Message
	for {
		msges, err := sqsClient.ReceiveMessage("test-queue")
		test.AssertNotError(t, err)
		if len(msges) == 0 {
			break
		}
		messages = append(messages, msges...)
	}

	for i, elem := range messages {
		test.AssertEqual(t, *elem.Body, "Testpayload"+strconv.Itoa(i))
	}
	test.AssertEqual(t, len(messages), n)
}
