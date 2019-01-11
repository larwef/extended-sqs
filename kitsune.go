package kitsune

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io/ioutil"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// AttributeNameS3Bucket is an attribute name used by sender to pass the location of messages put on S3 to the receiver.
	// Receiver uses this attribute if set to fetch a message from S3.
	AttributeNameS3Bucket = "payloadBucket"

	// AttributeNameKMSKey used to pass the KMS key used to encryptData the data key.
	AttributeNameKMSKey = "kmsKey"

	// AttributeCompression is used to signal that the payload is compressed
	AttributeCompression = "compression"
)

// Client object handles communication with SQS
type Client struct {
	opts options

	awsConfig aws.Config

	awsSQSClient *sqsClient
	awsS3Client  *s3Client
	awsKMSClient *kmsClient
}

type options struct {
	delaySeconds                int64
	maxNumberOfMessages         int64
	initialVisibilityTimeout    int64
	maxVisibilityTimeout        int64
	backoffFactor               int64
	backoffFunction             func(int64, int64, int64, int64) int64
	waitTimeSeconds             int64
	attributeNames              []*string
	messageAttributeNames       []*string
	s3Bucket                    string
	forceS3                     bool
	kmsKeyID                    string
	compressionEnabled          bool
	kmsKeyCacheEnabled          bool
	kmsKeyCacheExpirationPeriod time.Duration
}

var defaultClientOptions = options{
	delaySeconds:                30,
	maxNumberOfMessages:         10,
	initialVisibilityTimeout:    60,
	backoffFactor:               2,
	maxVisibilityTimeout:        900,
	waitTimeSeconds:             20,
	attributeNames:              []*string{aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount)},
	messageAttributeNames:       []*string{aws.String(AttributeNameS3Bucket), aws.String(AttributeNameKMSKey), aws.String(AttributeCompression)},
	forceS3:                     false,
	compressionEnabled:          false,
	kmsKeyCacheEnabled:          false,
	kmsKeyCacheExpirationPeriod: 5 * time.Minute,
}

// ClientOption sets configuration options for a awsSQSClient.
type ClientOption func(*options)

// DelaySeconds is used to set the DelaySeconds property on the awsSQSClient which is how many seconds the message will be
// unavaible once its put on a queue.
func DelaySeconds(d int64) ClientOption {
	return func(o *options) { o.delaySeconds = d }
}

// MaxNumberOfMessages sets the maximum number of messages can be returned each time the awsSQSClient fetches messages.
func MaxNumberOfMessages(m int64) ClientOption {
	return func(o *options) { o.maxNumberOfMessages = m }
}

// InitialVisibilityTimeout sets the initial time used when changing message visibility. The length of subsequent changes will be
// determined by strategy defined by the backoff function used.
func InitialVisibilityTimeout(i int64) ClientOption {
	return func(o *options) { o.initialVisibilityTimeout = i }
}

// MaxVisibilityTimeout sets the maxiumum time a message can be made unavailable by chaning message visibility.
func MaxVisibilityTimeout(m int64) ClientOption {
	return func(o *options) { o.maxVisibilityTimeout = m }
}

// BackoffFactor sets the backoff factor which is a paramter used by the backoff function.
func BackoffFactor(b int64) ClientOption {
	return func(o *options) { o.backoffFactor = b }
}

// BackoffFunction sets the function which computes the next visibility timeout.
func BackoffFunction(f func(int64, int64, int64, int64) int64) ClientOption {
	return func(o *options) { o.backoffFunction = f }
}

// WaitTimeSeconds sets the time a client will wait for messages on each call.
func WaitTimeSeconds(w int64) ClientOption {
	return func(o *options) { o.waitTimeSeconds = w }
}

// AttributeNames sets the attributes to be returned when fetching messages. ApproximateReceiveCount is always returned because it
// is used when calculating backoff.
func AttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for i := range s {
			o.attributeNames = append(o.attributeNames, &s[i])
		}
	}
}

// MessageAttributeNames sets the message attributes to be returned when fetching messages. ApproximateReceiveCount is always
// returned because it is used when calculating backoff.
func MessageAttributeNames(s ...string) ClientOption {
	return func(o *options) {
		for i := range s {
			o.messageAttributeNames = append(o.messageAttributeNames, &s[i])
		}
	}
}

// S3Bucket sets the bucket where the client should put messages which exceed max size.
func S3Bucket(s string) ClientOption {
	return func(o *options) { o.s3Bucket = s }
}

// ForceS3 set if all messages should be saved to S3.
func ForceS3(b bool) ClientOption {
	return func(o *options) { o.forceS3 = b }
}

// KMSKeyID sets the KMS key to be used for encryption.
func KMSKeyID(s string) ClientOption {
	return func(o *options) { o.kmsKeyID = s }
}

// CompressionEnabled is used to enable or disable compression of payload.
func CompressionEnabled(b bool) ClientOption {
	return func(o *options) { o.compressionEnabled = b }
}

// KMSKeyCacheEnabled used to enable or disable kms key caching. Note that caching is against best practise, but might provide
// significant savings by reducing calls to KMS.
func KMSKeyCacheEnabled(b bool) ClientOption {
	return func(o *options) { o.kmsKeyCacheEnabled = b }
}

// KMSKeyCacheExpirationPeriod sets the amount of time an entry in the kms key cache will be valid.
func KMSKeyCacheExpirationPeriod(t time.Duration) ClientOption {
	return func(o *options) { o.kmsKeyCacheExpirationPeriod = t }
}

// New returns a new awsSQSClient with configuration set as defined by the ClientOptions. Will create a s3Client from the
// aws.Config if a bucket is set. Same goes for KMS.
func New(awsConfig *aws.Config, opt ...ClientOption) (*Client, error) {
	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("Error getting AWS awsSession: %v ", err)
	}

	opts := defaultClientOptions
	for _, o := range opt {
		o(&opts)
	}

	sqsc := newSQSClient(sqs.New(awsSession), &opts)

	var s3c *s3Client
	if opts.s3Bucket != "" {
		s3c = newS3Client(s3.New(awsSession))
	}

	var kmsc *kmsClient
	if opts.kmsKeyID != "" {
		kmsc = newKMSClient(kms.New(awsSession), &opts)
	}

	return &Client{
		opts:         opts,
		awsSQSClient: sqsc,
		awsS3Client:  s3c,
		awsKMSClient: kmsc,
	}, nil
}

// SendMessage sends a message to the specified queue. Convenient method for sending a message without custom attributes. This
// does not guarantee there will be no attributes on the message to SQS. The client might add attributes eg. for file events when
// the payload is uploaded to S3.
func (c *Client) SendMessage(queueName *string, payload []byte) error {
	return c.SendMessageWithAttributes(queueName, payload, nil)
}

// SendMessageWithAttributes sends a message to the specified queue with attributes. If the message size exceeds maximum, the
// payload will be uploaded to the configured S3 bucket and a file event will be sent on the SQS queue. The bucket where the
// message was uploaded if put on the message attributes. This means an no of attributes error can be thrown even though this
// function is called with less than maximum number of attributes.
func (c *Client) SendMessageWithAttributes(queueName *string, payload []byte, messageAttributes map[string]*sqs.MessageAttributeValue) error {
	event := &sqsSendEvent{
		payload:           payload,
		messageAttributes: messageAttributes,
	}

	// Compress payload if compression is enabled
	if c.opts.compressionEnabled {
		if err := compress(event); err != nil {
			return err
		}
	}

	// Encrypt the payload if a KMS key is configured
	if c.opts.kmsKeyID != "" {
		if err := c.encrypt(event); err != nil {
			return err
		}
	}

	// Put payload to S3 if S3 is forced of message is larger than max size. Bucket needs to be configured.
	if (c.opts.forceS3 || event.size() > maxMessageSize) && c.opts.s3Bucket != "" {
		if err := c.uploadToS3(event); err != nil {
			return err
		}
	}

	return c.awsSQSClient.sendMessage(queueName, event)
}

// SendMessageBatch sends a batch of messages to SQS. Messages are compressed, encrypted and sent to S3 in the same way they would
// if sent as a single message. Note thay rrrors caught before sending to SQS are added to the sqs.SendMessageBatchOutput. So all
// entries in the Failed slice is not necessarily from calling SQS.
func (c *Client) SendMessageBatch(queueName *string, batch *Batch) (*sqs.SendMessageBatchOutput, error) {
	if atomic.LoadUint32(&batch.sent) == 1 {
		return nil, ErrorBatchAlreadySent
	}

	batch.m.Lock()
	defer batch.m.Unlock()
	if batch.sent == 0 {
		defer atomic.StoreUint32(&batch.sent, 1)
	} else {
		return nil, ErrorBatchAlreadySent
	}

	var entries []*sqs.SendMessageBatchRequestEntry
	var failed []*sqs.BatchResultErrorEntry
	for _, event := range batch.events {
		// Compress payload if compression is enabled
		if c.opts.compressionEnabled {
			if err := compress(event); err != nil {
				failed = append(failed, getBatchResultError(&event.id, err))
				continue
			}
		}

		// Encrypt the payload if a KMS key is configured
		if c.opts.kmsKeyID != "" {
			if err := c.encrypt(event); err != nil {
				failed = append(failed, getBatchResultError(&event.id, err))
				continue
			}
		}

		// Put payload to S3 if S3 is forced of message is larger than max size. Bucket needs to be configured.
		if (c.opts.forceS3 || event.size() > maxMessageSize) && c.opts.s3Bucket != "" {
			if err := c.uploadToS3(event); err != nil {
				failed = append(failed, getBatchResultError(&event.id, err))
				continue
			}
		}

		batchRequestEntry, err := c.awsSQSClient.bacthRequestEntry(event)
		if err != nil {
			failed = append(failed, getBatchResultError(&event.id, err))
		} else {
			entries = append(entries, batchRequestEntry)
		}
	}

	batchOutput, err := c.awsSQSClient.sendMessageBatch(queueName, entries)
	batchOutput.Failed = append(batchOutput.Failed, failed...)
	return batchOutput, err
}

func compress(s *sqsSendEvent) error {
	if compressed, err := compressData(s.payload); err == nil {
		s.payload = compressed
	} else {
		return err
	}

	if s.messageAttributes == nil {
		s.messageAttributes = make(map[string]*sqs.MessageAttributeValue)
	}
	s.messageAttributes[AttributeCompression] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("gzip")}

	return nil
}

func (c *Client) encrypt(s *sqsSendEvent) error {
	encryptedEvent, err := c.awsKMSClient.encrypt(&c.opts.kmsKeyID, s.payload)
	if err != nil {
		return fmt.Errorf("error encrypting payload: %v", err)
	}

	encryptedEventBytes, err := json.Marshal(encryptedEvent)
	if err != nil {
		return err
	}

	s.payload = encryptedEventBytes
	if s.messageAttributes == nil {
		s.messageAttributes = make(map[string]*sqs.MessageAttributeValue)
	}
	s.messageAttributes[AttributeNameKMSKey] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: &c.opts.kmsKeyID}

	return nil
}

func (c *Client) uploadToS3(s *sqsSendEvent) error {
	fileEvent, err := c.awsS3Client.putObject(&c.opts.s3Bucket, s.payload)
	if err != nil {
		return fmt.Errorf("error puting object to S3: %v", err)
	}

	fileEventBytes, err := json.Marshal(&fileEvent)
	if err != nil {
		return err
	}

	s.payload = fileEventBytes
	if s.messageAttributes == nil {
		s.messageAttributes = make(map[string]*sqs.MessageAttributeValue)
	}
	s.messageAttributes[AttributeNameS3Bucket] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: &c.opts.s3Bucket}

	return err
}

// The compressed string is base64 encoded because the compressed data might contain characters that are invalid and SQS would
// throw an error
func compressData(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	if _, err := zw.Write(payload); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(buf.Bytes())))
	base64.StdEncoding.Encode(encoded, buf.Bytes())
	return encoded, nil
}

// ReceiveSuccessfulEntry represents a sucessfully received SQS message
type ReceiveSuccessfulEntry struct {
	Message *sqs.Message
}

// ReceiveFailedEntry represents a received SQS message which failed when decompressing, decrypting or retrieving from S3.
type ReceiveFailedEntry struct {
	Message *sqs.Message
	Error   error
}

// ReceiveMessagesResult is used to collect the results of a batch receive
type ReceiveMessagesResult struct {
	Successful []*ReceiveSuccessfulEntry
	Failed     []*ReceiveFailedEntry
}

// ReceiveMessages polls the specified queue and returns the fetched messages. If the S3 bucket attribute is set, the payload is
// fetched and replaces the file event in the sqs.Message body. This will not delete the object in S3. A lifecycle rule is
// recomended.
func (c *Client) ReceiveMessages(queueName *string) (*ReceiveMessagesResult, error) {
	messages, err := c.awsSQSClient.receiveMessage(queueName)
	if err != nil {
		return nil, err
	}

	result := &ReceiveMessagesResult{}

	// Loop through messages and check if payload is located in S3 and/or if its encrypted.
	for _, message := range messages {
		// If S3 bucket is included the payload is located in S3 an needs to be fetched
		if _, exists := message.MessageAttributes[AttributeNameS3Bucket]; exists && c.awsS3Client != nil {
			var fe fileEvent
			if err := json.Unmarshal([]byte(*message.Body), &fe); err != nil {
				result.Failed = append(result.Failed, &ReceiveFailedEntry{
					Message: message,
					Error:   err,
				})
				continue
			}

			if payload, err := c.awsS3Client.getObject(&fe); err == nil {
				message.Body = aws.String(string(payload))
				delete(message.MessageAttributes, AttributeNameS3Bucket)
			} else {
				result.Failed = append(result.Failed, &ReceiveFailedEntry{
					Message: message,
					Error:   err,
				})
				continue
			}
		}

		// If KMS key is included the payload is encrypted and needs to be decrypted
		if _, exists := message.MessageAttributes[AttributeNameKMSKey]; exists && c.awsKMSClient != nil {
			var ee encryptedEvent
			if err := json.Unmarshal([]byte(*message.Body), &ee); err != nil {
				result.Failed = append(result.Failed, &ReceiveFailedEntry{
					Message: message,
					Error:   err,
				})
				continue
			}

			if decrypted, err := c.awsKMSClient.decrypt(&ee); err == nil {
				decryptedStr := string(decrypted)
				message.Body = &decryptedStr
				delete(message.MessageAttributes, AttributeNameKMSKey)
			} else {
				result.Failed = append(result.Failed, &ReceiveFailedEntry{
					Message: message,
					Error:   err,
				})
				continue
			}
		}

		// If compression key is included the payload needs to be decompressed
		if _, exists := message.MessageAttributes[AttributeCompression]; exists {
			buf := []byte(*message.Body)
			if decompressed, err := decompressData(buf); err == nil {
				message.Body = aws.String(string(decompressed))
				delete(message.MessageAttributes, AttributeCompression)
			} else {
				result.Failed = append(result.Failed, &ReceiveFailedEntry{
					Message: message,
					Error:   err,
				})
				continue
			}
		}

		result.Successful = append(result.Successful, &ReceiveSuccessfulEntry{
			Message: message,
		})
	}

	return result, nil
}

func decompressData(payload []byte) ([]byte, error) {
	base64Text := make([]byte, base64.StdEncoding.DecodedLen(len(payload)))
	l, err := base64.StdEncoding.Decode(base64Text, payload)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(base64Text[:l])
	zr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	decompressedBytes, err := ioutil.ReadAll(zr)
	if err != nil {
		return nil, err
	}

	if err := zr.Close(); err != nil {
		return nil, err
	}

	return decompressedBytes, nil
}

// ChangeMessageVisibility changes the visibilty of a message. Essentialy putting it back in the queue and unavailable for a
// specified amount of time.
func (c *Client) ChangeMessageVisibility(queueName *string, message *sqs.Message, timeout int64) error {
	return c.awsSQSClient.changeMessageVisibility(queueName, message, timeout)
}

// Backoff is used for changing message visibility based on a calculated amount of time determined by a back off function
// configured on the awsSQSClient.
func (c *Client) Backoff(queueName *string, message *sqs.Message) error {
	receivedCount, err := strconv.Atoi(*message.Attributes["ApproximateReceiveCount"])
	if err != nil {
		return errors.New("error getting received count")
	}

	receivedCount64 := int64(receivedCount)

	timeout := c.opts.backoffFunction(receivedCount64, c.opts.initialVisibilityTimeout, c.opts.maxVisibilityTimeout, c.opts.backoffFactor)
	return c.awsSQSClient.changeMessageVisibility(queueName, message, timeout)
}

// DeleteMessage removes a message from the queue.
func (c *Client) DeleteMessage(queueName *string, receiptHandle *string) error {
	return c.awsSQSClient.deleteMessage(queueName, receiptHandle)
}

// ExponentialBackoff can be configured on a client to achieve an exponential backoff strategy based on how many times the message
// is received.
func ExponentialBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	receiveCount := min(retryCount, 9999)
	retryNumber := max(receiveCount-1, 0)
	expTimeout := int64(math.Pow(float64(backoffFactor), float64(retryNumber)) * float64(minBackoff))

	return min(expTimeout, maxBackof)
}

// LinearBackoff can be configured on a awsSQSClient to achieve a linear backoff strategy based on how many times a message is
// received.
func LinearBackoff(retryCount, minBackoff, maxBackof, backoffFactor int64) int64 {
	return min(minBackoff+(retryCount-1)*backoffFactor, maxBackof)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

// ErrorMaxBatchSizeExceeded is returned when trying to add more to a batch than is allowed
var ErrorMaxBatchSizeExceeded = fmt.Errorf("maximum batch size of %d exceeded", maxBatchSize)

// ErrorIDNotUnique is returned when adding a message to a batch with the same id as another message
var ErrorIDNotUnique = errors.New("all ids in a batch needs to be unique")

// ErrorBatchAlreadySent is returned when trying to send a batch which is already sent.
var ErrorBatchAlreadySent = errors.New("all ids in a batch needs to be unique")

// Batch is used to aggregate messages and send as a batch to SQS.
type Batch struct {
	size   int
	ids    map[string]struct{}
	events []*sqsSendEvent
	m      sync.Mutex
	sent   uint32
}

// NewBatch returns a new Batch object.
func NewBatch() *Batch {
	return &Batch{
		ids: make(map[string]struct{}),
	}
}

// Size returns the size of a batch.
func (b *Batch) Size() int {
	return b.size
}

// Add adds a message to a batch. Returns the number of messages in the batch and error.
func (b *Batch) Add(payload []byte, id string, messageAttributes map[string]*sqs.MessageAttributeValue) (int, error) {
	if b.size >= maxBatchSize {
		return b.size, ErrorMaxBatchSizeExceeded
	}

	if _, exists := b.ids[id]; exists {
		return b.size, ErrorIDNotUnique
	}

	event := &sqsSendEvent{
		payload:           payload,
		messageAttributes: messageAttributes,
		id:                id,
	}

	b.events = append(b.events, event)

	b.size++
	b.ids[id] = struct{}{}
	return b.size, nil
}
