package kitsune

import (
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
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

func getClient(awsSQS sqsiface.SQSAPI, awsS3 s3iface.S3API, awsKMS kmsiface.KMSAPI, opt ...ClientOption) *Client {

	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	return &Client{
		awsSQSClient: &sqsClient{awsSQS: awsSQS, opts: &opts},
		awsS3Client:  &s3Client{awsS3: awsS3},
		awsKMSClient: &kmsClient{awsKMS: awsKMS},
		opts:         opts,
	}
}

func sendNMessages(t *testing.T, n int) {
	sqsMock := test.NewSQSMock(5, int64(n+10))
	sqsClient := getClient(sqsMock, nil, nil)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	for i := 0; i < n; i++ {
		err := sqsClient.SendMessage(&testQueue, "Testpayload"+strconv.Itoa(i))
		test.AssertNotError(t, err)
	}

	messages, err := sqsMock.WaitUntilMessagesReceived(&testQueue, n)
	test.AssertNotError(t, err)

	for i := 0; i < n; i++ {
		test.AssertEqual(t, *messages[i].Body, "Testpayload"+strconv.Itoa(i))
	}
}

func TestClient_SendMessage_MaxSize(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil)

	payload, err := ioutil.ReadFile("test/testdata/size262144Bytes.txt")
	test.AssertNotError(t, err)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	err = sqsClient.SendMessage(&testQueue, string(payload))
	test.AssertNotError(t, err)

	_, err = sqsMock.WaitUntilMessagesReceived(&testQueue, 1)
	test.AssertNotError(t, err)
}

func TestClient_SendMessageWithAttributes_MaxSize(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil)

	payload, err := ioutil.ReadFile("test/testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes(&testQueue, string(payload), attributes)
	test.AssertNotError(t, err)

	_, err = sqsMock.WaitUntilMessagesReceived(&testQueue, 1)
	test.AssertNotError(t, err)
}

func TestClient_SendMessage_OverMaxSizeS3NotConfigured(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil)

	payload, err := ioutil.ReadFile("test/testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	err = sqsClient.SendMessage(&testQueue, string(payload))
	test.AssertIsError(t, err)
	test.AssertEqual(t, err, ErrorMaxMessageSizeExceeded)
}

func TestClient_SendMessageWithAttributes_OverMaxSizeS3NotConfigured(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil)

	payload, err := ioutil.ReadFile("test/testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes(&testQueue, string(payload), attributes)
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

	sqsClient := getClient(sqsMock, s3Mock, nil, S3Bucket("test-bucket"))
	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	err = sqsClient.SendMessage(&testQueue, string(payload))
	test.AssertNotError(t, err)

	test.AssertEqual(t, s3Mock.PutObjectHandlerCalledCount, 1)
	message, err := sqsMock.WaitUntilMessagesReceived(&testQueue, 1)

	test.AssertEqual(t, *message[0].MessageAttributes[AttributeNameS3Bucket].StringValue, "test-bucket")

	var fe fileEvent
	if err := json.Unmarshal([]byte(*message[0].Body), &fe); err != nil {
		t.Fatalf("Error unmarshaling response: %v", err)
	}

	test.AssertEqual(t, *fe.Size, int64(262145))
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
	sqsClient := getClient(sqsMock, s3Mock, nil, S3Bucket("test-bucket"))
	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["a1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaa")}
	attributes["a4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("Aaaaaaaaa")}

	err = sqsClient.SendMessageWithAttributes(&testQueue, string(payload), attributes)
	test.AssertNotError(t, err)

	test.AssertEqual(t, s3Mock.PutObjectHandlerCalledCount, 1)
	message, err := sqsMock.WaitUntilMessagesReceived(&testQueue, 1)

	test.AssertEqual(t, *message[0].MessageAttributes[AttributeNameS3Bucket].StringValue, "test-bucket")

	var fe fileEvent
	if err := json.Unmarshal([]byte(*message[0].Body), &fe); err != nil {
		t.Fatalf("Error unmarshaling response: %v", err)
	}

	test.AssertEqual(t, *fe.Size, int64(262080))
}

func TestClient_SendMessageWithAttributes_MaxNoOfAttributesExceeded(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil)

	payload := "TestPayload"

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

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

	err := sqsClient.SendMessageWithAttributes(&testQueue, payload, attributes)
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
	sqsClient := getClient(sqsMock, nil, nil)

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	for i := 0; i < n; i++ {
		sendMessageInput := &sqs.SendMessageInput{
			MessageBody: aws.String("Testpayload" + strconv.Itoa(i)),
			QueueUrl:    &testQueue,
		}
		_, err := sqsMock.SendMessage(sendMessageInput)
		test.AssertNotError(t, err)
	}

	var messages []*sqs.Message
	for {
		msges, err := sqsClient.ReceiveMessage(&testQueue)
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
	sqsClient := getClient(sqsMock, s3Mock, nil, S3Bucket("test-bucket"))
	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	// Create and marshal fileEvent and put the file event on the SQS mock.
	fe := &fileEvent{
		Size:     aws.Int64(int64(262145)),
		Bucket:   aws.String("test-bucket"),
		Filename: aws.String("testFile"),
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
		QueueUrl:          &testQueue,
		MessageAttributes: messageAttributes,
	}
	_, err = sqsMock.SendMessage(sendMessageInput)
	test.AssertNotError(t, err)

	// Use client to fetch message. Verify that the payload in the message is the payload from S3 and not the file event put on
	// the queue.
	messages, err := sqsClient.ReceiveMessage(&testQueue)
	test.AssertNotError(t, err)
	test.AssertEqual(t, *messages[0].Body, string(payload))
}

func TestClient_SendMessage_KMS(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	kmsMock := &test.KmsMock{}
	sqsClient := getClient(sqsMock, nil, kmsMock, KMSKeyID("keyID"))

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	err := sqsClient.SendMessage(&testQueue, "TestPayload")
	test.AssertNotError(t, err)

	message, err := sqsMock.WaitUntilMessagesReceived(&testQueue, 1)
	test.AssertNotError(t, err)

	test.AssertEqual(t, kmsMock.GenerateDataKeyCalledCount, 1)

	var ee encryptedEvent
	err = json.Unmarshal([]byte(*message[0].Body), &ee)

	data, err := test.DecryptData(ee.Payload)
	test.AssertNotError(t, err)

	test.AssertEqual(t, string(data), "TestPayload")
}

func TestClient_ReceiveMessage_KMS(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	kmsMock := &test.KmsMock{}

	sqsClient := getClient(sqsMock, nil, kmsMock, KMSKeyID("keyID"))
	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	ee := encryptedEvent{
		EncryptedEncryptionKey: []byte{1, 2, 3, 0, 120, 117, 254, 40, 232, 55, 217, 84, 174, 75, 59, 2, 126, 80, 228, 40, 30, 154, 9, 181, 250, 93, 124, 148, 204, 162, 114, 168, 221, 48, 205, 156, 51, 1, 175, 64, 33, 234, 59, 118, 135, 116, 69, 35, 55, 119, 205, 97, 34, 121, 0, 0, 0, 126, 48, 124, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 6, 160, 111, 48, 109, 2, 1, 0, 48, 104, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 1, 48, 30, 6, 9, 96, 134, 72, 1, 101, 3, 4, 1, 46, 48, 17, 4, 12, 206, 50, 137, 217, 81, 233, 59, 171, 93, 91, 58, 132, 2, 1, 16, 128, 59, 113, 1, 196, 78, 149, 34, 228, 82, 195, 190, 192, 248, 95, 192, 11, 108, 200, 117, 57, 165, 152, 54, 95, 169, 176, 53, 71, 217, 119, 34, 10, 26, 143, 240, 124, 202, 53, 167, 94, 151, 240, 14, 124, 48, 154, 153, 11, 46, 189, 202, 163, 21, 232, 230, 10, 202, 105, 98, 175},
		KeyID:                  "KeyID",
		Payload:                []byte{102, 205, 159, 72, 200, 107, 205, 104, 53, 185, 73, 108, 171, 58, 12, 177, 114, 113, 72, 157, 2, 234, 89, 248, 169, 25, 247, 54, 249, 249, 189, 35, 226, 248, 252, 167, 110, 245, 127},
	}

	eebytes, err := json.Marshal(ee)
	test.AssertNotError(t, err)

	messageAttributes := make(map[string]*sqs.MessageAttributeValue)
	messageAttributes[AttributeNameKMSKey] = &sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("keyID"),
	}

	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(string(eebytes)),
		QueueUrl:          &testQueue,
		MessageAttributes: messageAttributes,
	}
	_, err = sqsMock.SendMessage(sendMessageInput)
	test.AssertNotError(t, err)

	messages, err := sqsClient.ReceiveMessage(&testQueue)
	test.AssertNotError(t, err)
	test.AssertEqual(t, *messages[0].Body, "TestPayload")
}

func TestClient_SendMessage_Compressed(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))
	sqsClient := getClient(sqsMock, nil, nil, CompressionEnabled(true))

	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	err := sqsClient.SendMessage(&testQueue, "TestPayload")
	test.AssertNotError(t, err)

	message, err := sqsMock.WaitUntilMessagesReceived(&testQueue, 1)
	test.AssertNotError(t, err)

	decompressed, err := decompress(*message[0].Body)
	test.AssertNotError(t, err)
	test.AssertEqual(t, decompressed, "TestPayload")
}

func TestClient_ReceiveMessage_Compressed(t *testing.T) {
	sqsMock := test.NewSQSMock(5, int64(10))

	sqsClient := getClient(sqsMock, nil, nil, CompressionEnabled(true))
	testQueue := "test-queue"
	sqsMock.CreateQueueIfNotExists(&testQueue)

	compressed, err := compress("TestPayload")
	test.AssertNotError(t, err)

	messageAttributes := make(map[string]*sqs.MessageAttributeValue)
	messageAttributes[AttributeCompression] = &sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("gzip"),
	}
	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:       &compressed,
		QueueUrl:          &testQueue,
		MessageAttributes: messageAttributes,
	}

	_, err = sqsMock.SendMessage(sendMessageInput)
	test.AssertNotError(t, err)

	messages, err := sqsClient.ReceiveMessage(&testQueue)
	test.AssertNotError(t, err)
	test.AssertEqual(t, *messages[0].Body, "TestPayload")
}
