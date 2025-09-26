# Event-Driven Data Processing Architecture (AWS Lambda + S3 + SQS + DynamoDB + IAM)

This project is a serverless event-driven data processing system built with AWS services and Python.

---

## **Overview**

* **S3 Bucket**: Stores uploaded CSV files and triggers Lambda on new objects.
* **SQS Queue**: Receives messages and triggers Lambda.
* **Lambda Function**: Processes events from S3 and SQS, validates and cleanses data, logs warnings, and stores processed data in DynamoDB.
* **DynamoDB Table**: Stores cleaned and validated user data with `user_id` as the primary key.
* **CloudWatch Logs**: Captures warnings, errors, and processing logs.
* **IAM Roles**: Lambda has permissions for S3, DynamoDB, and SQS access.

---

## **Tech Stack**

* Python 3.10
* AWS CDK v2 (`aws-cdk-lib`)
* AWS Lambda
* S3
* SQS
* DynamoDB
* CloudWatch
* Pandas (via Lambda Layer)

---

## **How It Works**

1. CSV file uploaded to S3 → triggers Lambda.
2. Message sent to SQS → triggers Lambda.
3. Lambda:

   * Reads CSV or SQS message.
   * Validates data (emails, age, missing fields).
   * Cleanses data (normalize email, handle missing values).
   * Logs invalid data as warnings.
   * Stores valid data in DynamoDB.

---

## **Testing**

* CDK stack tests written with `aws-cdk-lib.assertions`.
* Tests validate creation of Lambda, SQS, S3, and DynamoDB resources.
* **Note:** There’s a complex dependency conflict with PyTest and CDK packages, so tests may fail unless dependencies are carefully managed.

---

## **Deployment**

1. Clone the repo.
2. Install dependencies in a virtual environment.
3. Ensure you have configured you AWS access credentials.
4. Deploy stack with:

```bash
cdk deploy
```

---

## Author

Ahmad Al Sharbaji

I work on data and backend systems.

I hope this repo helps. If you ran the tests and PyTest worked on your machine, I’d appreciate a brief note about your setup and any issues you hit.

Thank you ^_^

