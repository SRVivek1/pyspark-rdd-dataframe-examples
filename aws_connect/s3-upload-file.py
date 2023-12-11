import boto3
import creds_mgr as creds
"""
    Upload file to s3.
    
    Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
"""

s3 = boto3.resource('s3', aws_access_key_id=creds.AWS_ACCESS_KEY_ID, aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY)

# List all buckets
print("----------------------- s3.buckets.all() ---------------------------------")
for bkts in s3.buckets.all():
    print(bkts.name)


print("----------------------- s3.create_bucket(Bucket='vsingh-dev.aws.poc.test.data') ---------------------------------")
# Create a new bucket
bucket = s3.create_bucket(Bucket='vsingh-dev.aws.poc.test.data')
print(bucket)



# Upload file
print("-------------------- s3.Bucket('vsingh-dev.aws.poc.test.data').put_object(Key='test-doc.txt', Body=data) ------------------")
with open('test-doc.txt', 'rb') as data:
    s3.Bucket('vsingh-dev.aws.poc.test.data').put_object(Key='test-doc.txt', Body=data)


# List all buckets
print("----------------------- s3.buckets.all() ---------------------------------")
for bkts in s3.buckets.all():
    print(bkts.name)

