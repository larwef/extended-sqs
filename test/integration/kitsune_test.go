// +build integration

package integration

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/larwef/kitsune"
	"github.com/larwef/kitsune/test"
	"io/ioutil"
	"testing"
)

var awsRegion = "eu-west-1"
var profile = "sqs_test_user"

var testQueueName = "sqs-client-test-queue"
var testBucket = "sqs-client-test-bucket"
var testKMSKey = "alias/sqs-client-test-key"

func getClient(t *testing.T, opts ...kitsune.ClientOption) *kitsune.Client {
	config := aws.Config{
		Region:      &awsRegion,
		Credentials: credentials.NewSharedCredentials("", profile),
	}

	options := []kitsune.ClientOption{
		kitsune.InitialVisibilityTimeout(5),
		kitsune.MaxVisibilityTimeout(10),
	}

	options = append(options, opts...)

	client, err := kitsune.New(&config, options...)
	test.AssertNotError(t, err)

	return client
}

// These tests need an empty queue to run

func TestClient_SendReceiveAndDeleteSingleMessage(t *testing.T) {
	sqsClient := getClient(t)

	payload := uuid.New().String()

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, payload); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s with Payload:\n%s", testQueueName, payload)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *messages[0].Body)

	if *messages[0].Body != payload {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)
}

func TestClient_SendReceiveAndDeleteSingleMessageWithAttributes(t *testing.T) {
	sqsClient := getClient(t, kitsune.MessageAttributeNames("attribute1", "attribute2", "attribute111"))

	payload := uuid.New().String()

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["attribute1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute1")}
	attributes["attribute2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute2")}
	attributes["attribute3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute3")}

	// Send message
	if err := sqsClient.SendMessageWithAttributes(&testQueueName, payload, attributes); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s with Payload:\n%s", testQueueName, payload)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *messages[0].Body)

	if *messages[0].Body != payload {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	test.AssertEqual(t, *messages[0].MessageAttributes["attribute1"].StringValue, "TestAttribute1")
	test.AssertEqual(t, *messages[0].MessageAttributes["attribute2"].StringValue, "TestAttribute2")
	_, exists := messages[0].MessageAttributes["attribute3"]
	test.AssertEqual(t, exists, false)

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)
}

func TestClient_ExtendVisibilityTimeout(t *testing.T) {
	sqsClient := getClient(t)

	payload := uuid.New().String()

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, payload); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s with Payload:\n%s", testQueueName, payload)

	// Receive message first time
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *messages[0].Body)

	message := messages[0]
	if *message.Body != payload {
		t.Fatalf("Expected: %s. Actual: %s", payload, *message.Body)
	}

	// Extend message visibility and receive n times
	// Keep wait time to under 20s to make this work
	var timeout int64 = 3
	for n := 0; n < 3; n++ {
		err = sqsClient.ChangeMessageVisibility(&testQueueName, message, timeout)
		if err != nil {
			t.Fatalf("Error extending visibility time: %v ", err)
		}

		t.Logf("Extended visibility timeout for message with receipt: %s", *message.ReceiptHandle)

		messages, err := sqsClient.ReceiveMessage(&testQueueName)
		if err != nil {
			t.Fatalf("Error receiving message from SQS: %v ", err)
		}

		t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *message.Body)

		message = messages[0]
		if *message.Body != payload {
			t.Fatalf("Expected: %s. Actual: %s", payload, *message.Body)
		}
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, message.ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *message.ReceiptHandle)
}

func TestClient_SendReceiveAndDeleteLargeMessage(t *testing.T) {
	sqsClient := getClient(t, kitsune.S3Bucket(testBucket))

	payload, err := ioutil.ReadFile("../testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, string(payload)); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s.\n", testQueueName)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s.\n", testQueueName)

	if *messages[0].Body != string(payload) {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)

	t.Logf("Payload:\n%s", *messages[0].Body)
}

func TestClient_SendReceiveAndDeleteLargeMessageWithAttributes(t *testing.T) {
	sqsClient := getClient(t, kitsune.S3Bucket(testBucket))

	payload, err := ioutil.ReadFile("../testdata/size262080Bytes.txt")
	test.AssertNotError(t, err)

	attributes := make(map[string]*sqs.MessageAttributeValue)

	attributes["attribute1"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute1")}
	attributes["attribute2"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute2")}
	attributes["attribute3"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute3")}
	attributes["attribute4"] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("TestAttribute4")}

	// Send message
	if err := sqsClient.SendMessageWithAttributes(&testQueueName, string(payload), attributes); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s.\n", testQueueName)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s.\n", testQueueName)

	if *messages[0].Body != string(payload) {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)

	t.Logf("Payload:\n%s", *messages[0].Body)
}

func TestClient_SendReceiveAndDeleteSingleMessage_KMS(t *testing.T) {
	sqsClient := getClient(t, kitsune.KMSKeyID(testKMSKey))

	payload := uuid.New().String()

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, payload); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s with Payload:\n%s", testQueueName, payload)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *messages[0].Body)

	if *messages[0].Body != payload {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)
}

func TestClient_SendReceiveAndDeleteLargeMessage_S3AndKMS(t *testing.T) {
	sqsClient := getClient(t, kitsune.S3Bucket(testBucket), kitsune.KMSKeyID(testKMSKey), kitsune.CompressionEnabled(true))

	payload, err := ioutil.ReadFile("../testdata/size262145Bytes.txt")
	test.AssertNotError(t, err)

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, string(payload)); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s.\n", testQueueName)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s.\n", testQueueName)

	if *messages[0].Body != string(payload) {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)

	t.Logf("Payload:\n%s", *messages[0].Body)
}

func TestClient_SendReceiveAndDeleteSingleMessage_CompressionEnabled(t *testing.T) {
	sqsClient := getClient(t, kitsune.CompressionEnabled(true))

	payload := uuid.New().String()

	// Send message
	if err := sqsClient.SendMessage(&testQueueName, payload); err != nil {
		t.Fatalf("Error sending message to SQS: %v ", err)
	}
	t.Logf("Sent message to queue: %s with Payload:\n%s", testQueueName, payload)

	// Receive message
	messages, err := sqsClient.ReceiveMessage(&testQueueName)
	if err != nil {
		t.Fatalf("Error receiving message from SQS: %v ", err)
	}

	t.Logf("Received message from SQS queue: %s with ayload:\n%s", testQueueName, *messages[0].Body)

	if *messages[0].Body != payload {
		t.Fatalf("Expected: %s. Actual: %s", payload, *messages[0].Body)
	}

	// Delete message
	err = sqsClient.DeleteMessage(&testQueueName, messages[0].ReceiptHandle)
	if err != nil {
		t.Fatalf("Error deleting message from SQS queue: %v", err)
	}

	t.Logf("Message with recept: %s deleted from SQS Queue", *messages[0].ReceiptHandle)
}
