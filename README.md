# Kitsune
Go client for AWS SQS.

## Planned features:
- [x] Support large payloads by using S3
- [x] Encrypt with KMS
    - [ ] Cache KMS key


## Policy suggestion
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