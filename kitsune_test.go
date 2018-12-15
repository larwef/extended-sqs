package kitsune

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/larwef/kitsune/test"
	"io/ioutil"
	"strconv"
	"testing"
)

func TestClient_SendMessage(t *testing.T) {
	for i := 1; i <= 100; i++ {
		sendNMessages(t, i)
	}
}

func TestClient_ReceiveMessage(t *testing.T) {
	for i := 1; i <= 100; i++ {
		receiveNMessages(t, i)
	}
}

func TestClient_SendMessage_MaxSize(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

	payload, err := ioutil.ReadFile("test/testdata/size262144Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock.CreateQueueIfNotExists("test-queue")

	err = sqsClient.SendMessage("test-queue", string(payload))
	test.AssertNotError(t, err)

	_, err = sqsMock.WaitUntilMessagesReceived("test-queue", 1)
	test.AssertNotError(t, err)
}

func TestClient_SendMessageWithAttributes_MaxSize(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

	payload, err := ioutil.ReadFile("test/testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock.CreateQueueIfNotExists("test-queue")

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes("test-queue", string(payload), attributes)
	test.AssertNotError(t, err)

	_, err = sqsMock.WaitUntilMessagesReceived("test-queue", 1)
	test.AssertNotError(t, err)
}

func TestClient_SendMessage_OverMaxSizeS3NotConfigured(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

	payload, err := ioutil.ReadFile("test/testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock.CreateQueueIfNotExists("test-queue")

	err = sqsClient.SendMessage("test-queue", string(payload))
	test.AssertIsError(t, err)
	test.AssertEqual(t, err, ErrorMaxMessageSizeExceeded)
}

func TestClient_SendMessageWithAttributes_OverMaxSizeS3NotConfigured(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

	payload, err := ioutil.ReadFile("test/testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock.CreateQueueIfNotExists("test-queue")

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes("test-queue", string(payload), attributes)
	test.AssertIsError(t, err)
	test.AssertEqual(t, err, ErrorMaxMessageSizeExceeded)
}

func TestClient_SendMessage_OverMaxSize(t *testing.T) {

}

func TestClient_SendMessageWithAttributes_MaxNoOfAttributesExceeded(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

	payload := "TestPayload"

	sqsMock.CreateQueueIfNotExists("test-queue")

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute1")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute2")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute3")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute4")}
	attributes["a5"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute5")}
	attributes["a6"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute6")}
	attributes["a7"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute7")}
	attributes["a8"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute8")}
	attributes["a9"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute9")}
	attributes["a10"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute10")}
	attributes["a11"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute11")}

	err := sqsClient.SendMessageWithAttributes("test-queue", payload, attributes)
	test.AssertIsError(t, err)
	test.AssertEqual(t, err, ErrorMaxNumberOfAttributesExceeded)
}

// Helper functions
func getClient(awsSQS sqsiface.SQSAPI, opts options) *Client {
	return &Client{
		awsSQSClient: &sqsClient{
			awsSQS: awsSQS,
			opts:   &opts,
		},
		opts: opts,
	}
}

func sendNMessages(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

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

func receiveNMessages(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := getClient(sqsMock, defaultClientOptions)

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
