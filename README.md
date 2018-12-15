# Kitsune
Go client for AWS SQS.

Planned features:
- [x] Support large payloads by using S3
- [ ] Encrypt with KMS


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
            "Resource": "<queue ARN>"
        },
        {
            "Sid": "s3Permissions",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "<bucket ARN>*"
        }
    ]
}
```