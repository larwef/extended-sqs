[![Build Status](https://travis-ci.org/larwef/kitsune.svg?branch=master)](https://travis-ci.org/larwef/kitsune)
[![Go Report Card](https://goreportcard.com/badge/github.com/larwef/kitsune)](https://goreportcard.com/report/github.com/larwef/kitsune)

# Kitsune SQS Client
Go client for Amazon Simple Queue Service with support for messages larger than the max size of an SQS message by saving to S3 and
encryption using Key Management Service.

## Usage
All that is needed to make a client is an aws.Config

```
config := aws.Config{
		Region:      &awsRegion,
		Credentials: credentials.NewSharedCredentials("", profile),
	}
```

The client has a default set of options which should provide a good baseline. All the options can be overwritten using
ClientOptions. NB: S3 and KMS is not a part of the default configuration. The only thing thats needed is the bucket where the
client should save large payloads and the KMS key to be used for encryption.

```
options := []kitsune.ClientOption{
		kitsune.S3Bucket("myS3Bucket"),
		kitsune.KMSKeyID("myKMSKeyID"),
		kitsune.InitialVisibilityTimeout(10),
		kitsune.MaxVisibilityTimeout(100),
	}
```

Pass the aws.Config and ClientOptions to New and you're good to go. New will return an error if it was unable to get an
aws.Session

```
client, err := kitsune.New(&config, options...)
```

## Client Options
 See https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html for more details on some of the options.
 
| Option                      | Default                                   | Range                                                                         | Comment                                                                                                                                                                                                 |
| --------------------------- | :---------------------------------------: | :---------------------------------------------------------------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| delaySeconds                | 30s                                       | 0 - 900s                                                                      | Sets how many seconds a message will be unavailable before they are pollable from SQS.                                                                                                                  |
| maxNumberOfMessages         | 10                                        | 1 - 10                                                                        | Max number of messages returned when polling SQS.                                                                                                                                                       |
| initialVisibilityTimeout    | 60s                                       | 0 - 43,200 (12hours max)                                                      | Sets the minimum visibility timeout in seconds when calculating visibility timeout.                                                                                                                     |
| maxVisibilityTimeout        | 900s                                      | 0 - 43,200 (12hours max)                                                      | Sets max visibility timeout in seconds when calculating visibility timeout.                                                                                                                             |
| backoffFactor               | 2                                         | No limits. But should make sense in the function used for calculating backoff | Used when calculating visibility timeout.                                                                                                                                                               |
| backoffFunction             | not set                                   | N/A                                                                           | Function used for calculating next visibility timeout. One can implement one or use on of the provided functions.                                                                                       |
| waitTimeSeconds             | 20                                        | 1 - 20s                                                                       | Number of seconds a polling call will wait for response. Remeber to enable long polling when creating the queue.                                                                                        |
| attributeNames              | ApproximateReceiveCount                   | N/A                                                                           | Determines which (AWS specific) attributes are returned when polling SQS. ApproximateReceiveCount is used for backoff and is set by default.                                                            |
| messageAttributeNames       | The ones used for S3, KMS and compression | 0 - 7 (3 - 10) attributes. 3 used by the client.                              | Determines which (custom) attributes are returned when polling SQS. Remeber to add here if using any custom message attributes.                                                                         |
| s3Bucket                    | Not set ("")                              | N/A                                                                           | Determines which bucket payloads will be uploaded to. Remeber that sender and receiver might use different buckets. So make sure both have appropriate permissions.                                     |
| forceS3                     | false                                     | N/A                                                                           | All messages will be put to S3 regardless of size                                                                                                                                                       |
| kmsKeyID                    | Not set ("")                              | N/A                                                                           | Sets the KMS key usedfor encryption. Remember that the key used by sender and receiver is not necessarily the same. So each side needs to have permission for all keys used when sending and receiving. |
| compressionEnabled          | false                                     | N/A                                                                           | Payloads are gzip compressed.                                                                                                                                                                           |
| kmsKeyCacheEnabled          | false                                     | N/A                                                                           | If enabled keys will be kept in memory for a set duration and reused. Note that caching keys is against best practise, which is why it's disabled by default, but it can save a lot on calls to KMS.    |
| kmsKeyCacheExpirationPeriod | 5min                                      | N/A                                                                           | The duration a key in the cache will be valid if key caching is enabled                                                                                                                                 |
| skipSQSClient               | false                                     | N/A                                                                           | Used when Lambda has SQS trigger and you dont need to handle SQS communication. Dont use this if you want the Lambda to put messages on a queue (using this client).                                                    |

## Planned features:
- [x] Support large payloads by using S3
- [x] Encrypt and decrypt with KMS
    - [x] Cache KMS key
- [x] Compression
- [ ] Batch sending with compression, encryption and large payloads to S3
- [ ] Simple message consumer which handles message lifecycle (receive, process, delete, backoff)

## Policy suggestion
Make sure the user/role has the appripriate permissions. This policy assumes there is one queue, one bucket and one key being
used. It is possible to use different buckets and keys with multiple clients. Just make sure to add all the resources to the
appropriate policy (assuming a sufficient policy is applied to the user/role).

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "sqsPermissions",
            "Effect": "Allow",
            "Action": [
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessage",
                "sqs:GetQueueUrl"
            ],
            "Resource": "<SQS arn>"
        },
        {
            "Sid": "s3Permissions",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "<S3 Bucket arn>*"
        },
        {
            "Sid": "kmsPermissions",
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey",
                "kms:Decrypt"
            ],
            "Resource": "<KMS Key arn>"
        }
    ]
}
```