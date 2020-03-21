import boto3
import math

ec2_client = boto3.client('ec2')

sqs_client = boto3.client('sqs')

userData= """#!/bin/bash
echo "Hello World" 
touch /tmp/test.txt"""


def start(instance_list):
	try:
		response = ec2_client.start_instances(
		    InstanceIds=instance_list,
		    DryRun=False
		)

		print(response)
	
	except Exception as err:
		print(err)


def stop(instance_list):
	try:
		response = ec2_client.stop_instances(
		    InstanceIds=instance_list,
		    DryRun=False
		)

		print(response)
	
	except Exception as err:
		print(err)


def create_instances(num):
	instance = ec2_client.run_instances(
		ImageId='ami-0903fd482d7208724',
		InstanceType='t2.micro',
		MinCount=1,
		MaxCount=num,
		KeyName="ec2-keypair",
		# Placement={
  #       'AvailabilityZone': 'us-east-1'
  #       },
  		SecurityGroupIds=['sg-01c87755a98782687'],
        DryRun=False,
        UserData=userData)

	print(instance)

def terminate_all_instances():
	response = ec2_client.terminate_instances(
		InstanceIds=fetch_instances("running"),
		DryRun=False
		)

	print(response)

def fetch_instances(status):
	response = ec2_client.describe_instances(
		DryRun=False)
	# print(response)
	instance_list = []
	for elem in response['Reservations']:
		for instance in elem["Instances"]:
			if instance["State"]["Name"]==status:
				instance_list.append(instance["InstanceId"])


	return instance_list
	# print(len(response['Reservations']))

def send_message_queue():
	response = sqs_client.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/056594258736/video-process',
    MessageBody='Test',
    MessageAttributes={
        'VideoId': {
            'StringValue': 'xVq175Ghkcm',
            'DataType': 'String'
        }
    }
	)

	# print(response)


def receive_message():
	response = sqs_client.receive_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/159840518220/video_queue',
    AttributeNames=[
        'All'
    ],
    MessageAttributeNames=[
        'All'
    ],
    MaxNumberOfMessages=1
	)

	print(response)


def get_queue_attributes():
	response = sqs_client.get_queue_attributes(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/159840518220/video_queue',
    AttributeNames=[
        'ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible'
    ]
	)

	return response

def delete_all_messages_queue():

	curr_messages = int(get_queue_attributes()["Attributes"]["ApproximateNumberOfMessages"])
	while curr_messages:

		response = sqs_client.receive_message(
			    QueueUrl='https://sqs.us-east-1.amazonaws.com/159840518220/video_queue',
			    MaxNumberOfMessages=1,
			    MessageAttributeNames=[
			        'All'
			    ],
			    VisibilityTimeout=0,
			    WaitTimeSeconds=0
			)


		message = response['Messages'][0]
		receipt_handle = message['ReceiptHandle']


		sqs_client.delete_message(
		    QueueUrl='https://sqs.us-east-1.amazonaws.com/159840518220/video_queue',
		    ReceiptHandle=receipt_handle
			)

		# print('Received and deleted message: %s' % message)

		curr_messages = int(get_queue_attributes()["Attributes"]["ApproximateNumberOfMessages"])
		print(curr_messages)



for i in range(4):
	send_message_queue()
# delete_all_messages_queue()

# poll_for_scaling()