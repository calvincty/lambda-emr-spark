# Overview #

A lambda function (trigger by API Gateway HTTP API endpoint) to spin up a EMR cluster to run a spark job and publish a message to SNS through bash script as notification on job completed.

![API Gateway HTTP API POST](./screenshots/api-post-request.png)


---

## Configurations ##
The EMR cluster configuration parameters are located at `utils/app_settings.py` (using environment variables in the Lambda function). Some of the parameteres could be override by the API payloads

## Lambda ##
Lambda function output in CloudWatch Logs

![Lambda function output](./screenshots/cloudwatch-log.png)

## EMR ##
The scripts located at `steps/` would be uploaded to the S3 before deploy to the AWS Lambda function in Github Action Workflow

> EMR Cluster

![EMR Cluster](./screenshots/emr-cluster.png)

> EMR Cluster Steps

![EMR Cluster](./screenshots/emr-steps.png)

> SNS Email Notification

![Notification](./screenshots/email-notification.png)

## Read Output in Athena ##

> Table Properties

![Athena Table Properties](./screenshots/athena-table-properties.png)

> Table rows

![Athena Table](./screenshots/athena-table.png)


## Github Action Workflow ##
* Upload spark and bash scripts to S3
* Deploy to AWS Lambda function on `main` branch commit pushed/merged

![EMR Cluster](./screenshots/github-action-workflow.png)

