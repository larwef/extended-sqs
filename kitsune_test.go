package kitsune

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
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

func getClient(awsSQS sqsiface.SQSAPI, awsS3 s3iface.S3API, opt ...ClientOption) *Client {

	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	return &Client{
		awsSQSClient: &sqsClient{awsSQS: awsSQS, opts: &opts},
		awsS3Client:  &s3Client{awsS3: awsS3},
		opts:         opts,
	}
}

func sendNMessages(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := getClient(sqsMock, nil)

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

func TestClient_SendMessage_MaxSize(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil)

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
	sqsClient := getClient(sqsMock, nil)

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
	sqsClient := getClient(sqsMock, nil)

	payload, err := ioutil.ReadFile("test/testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock.CreateQueueIfNotExists("test-queue")

	err = sqsClient.SendMessage("test-queue", string(payload))
	test.AssertIsError(t, err)
	test.AssertEqual(t, err, ErrorMaxMessageSizeExceeded)
}

func TestClient_SendMessageWithAttributes_OverMaxSizeS3NotConfigured(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil)

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
	payload, err := ioutil.ReadFile("test/testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock := test.NewSQSMock(5, int64(10))
	// Getting S3 mock and Setting handler function for GetObject. Verify that the payload sent to S3 is the same as the one
	// passed to the client send function.
	s3Mock := &test.S3Mock{}
	s3Mock.PutObjectHandler = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		s3Payload, err := ioutil.ReadAll(input.Body)
		test.AssertNotError(t, err)
		test.AssertEqual(t, string(s3Payload), string(payload))
		return &s3.PutObjectOutput{}, nil
	}

	sqsClient := getClient(sqsMock, s3Mock, S3Bucket("test-bucket"))
	sqsMock.CreateQueueIfNotExists("test-queue")

	err = sqsClient.SendMessage("test-queue", string(payload))
	test.AssertNotError(t, err)

	test.AssertEqual(t, s3Mock.PutObjectHandlerCalledCount, 1)
	message, err := sqsMock.WaitUntilMessagesReceived("test-queue", 1)

	test.AssertEqual(t, *message[0].MessageAttributes[AttributeNameS3Bucket].StringValue, "test-bucket")

	var fe fileEvent
	if err := json.Unmarshal([]byte(*message[0].Body), &fe); err != nil {
		t.Fatalf("Error unmarshaling response: %v", err)
	}

	test.AssertEqual(t, fe.Size, int64(262145))
}

func TestClient_SendMessageWithAttributes_OverMaxSize(t *testing.T) {
	payload, err := ioutil.ReadFile("test/testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock := test.NewSQSMock(5, int64(10))
	// Getting S3 mock and Setting handler function for GetObject. Verify that the payload sent to S3 is the same as the one
	// passed to the client send function.
	s3Mock := &test.S3Mock{}
	s3Mock.PutObjectHandler = func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		s3Payload, err := ioutil.ReadAll(input.Body)
		test.AssertNotError(t, err)
		test.AssertEqual(t, string(s3Payload), string(payload))
		return &s3.PutObjectOutput{}, nil
	}

	// Get client with SQS mock and S3 mock.
	sqsClient := getClient(sqsMock, s3Mock, S3Bucket("test-bucket"))
	sqsMock.CreateQueueIfNotExists("test-queue")

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes("test-queue", string(payload), attributes)
	test.AssertNotError(t, err)

	test.AssertEqual(t, s3Mock.PutObjectHandlerCalledCount, 1)
	message, err := sqsMock.WaitUntilMessagesReceived("test-queue", 1)

	test.AssertEqual(t, *message[0].MessageAttributes[AttributeNameS3Bucket].StringValue, "test-bucket")

	var fe fileEvent
	if err := json.Unmarshal([]byte(*message[0].Body), &fe); err != nil {
		t.Fatalf("Error unmarshaling response: %v", err)
	}

	test.AssertEqual(t, fe.Size, int64(262080))
}

func TestClient_SendMessageWithAttributes_MaxNoOfAttributesExceeded(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil)

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

func TestClient_ReceiveMessage(t *testing.T) {
	for i := 1; i <= 100; i++ {
		receiveNMessages(t, i)
	}
}

func receiveNMessages(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := getClient(sqsMock, nil)

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

func TestClient_ReceiveMessage_FileEvent(t *testing.T) {
	payload, err := ioutil.ReadFile("test/testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	sqsMock := test.NewSQSMock(5, int64(10))
	// Getting S3 mock and Setting handler function for GetObject. Check that the right bucket is called and with the right key.
	// Return a payload which should appear in the response instead of the fileEvent that was put on the queue.
	s3Mock := &test.S3Mock{}
	s3Mock.GetObjectHandler = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		test.AssertEqual(t, *input.Bucket, "test-bucket")
		test.AssertEqual(t, *input.Key, "testFile")
		bufferString := ioutil.NopCloser(bytes.NewBufferString(string(payload)))
		return &s3.GetObjectOutput{
			Body: bufferString,
		}, nil
	}

	// Get client with SQS mock and S3 mock.
	sqsClient := getClient(sqsMock, s3Mock, S3Bucket("test-bucket"))
	sqsMock.CreateQueueIfNotExists("test-queue")

	// Create and marshal fileEvent and put the file event on the SQS mock.
	fe := &fileEvent{
		Size:     int64(262145),
		Filename: "testFile",
	}

	febytes, err := json.Marshal(fe)
	test.AssertNotError(t, err)

	messageAttributes := make(map[string]*sqs.MessageAttributeValue)
	messageAttributes[AttributeNameS3Bucket] = &sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("test-bucket"),
	}

	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(string(febytes)),
		QueueUrl:          aws.String("test-queue"),
		MessageAttributes: messageAttributes,
	}
	_, err = sqsMock.SendMessage(sendMessageInput)
	test.AssertNotError(t, err)

	// Use client to fetch message. Verify that the payload in the message is the payload from S3 and not the file event put on
	// the queue.
	messages, err := sqsClient.ReceiveMessage("test-queue")
	test.AssertNotError(t, err)
	test.AssertEqual(t, *messages[0].Body, string(payload))
}
