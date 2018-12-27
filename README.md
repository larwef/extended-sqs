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


## Planned features:
- [x] Support large payloads by using S3
- [x] Encrypt and decrypt with KMS
    - [x] Cache KMS key
- [x] Compression
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