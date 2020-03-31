import boto3

ec2_client = boto3.client('ec2')
waiter = ec2_client.get_waiter('instance_running')


# region = "us-east-1"
# access_id = "ASIASKNZ4YBGDQGIML3B"
# access_key = "cAZFwjaFNXna4EDweKdkEmjSAZKWqUuL0Y5m0xhU"
# session_token = "FwoGZXIvYXdzEJP//////////wEaDFv3PsIyytOIKZLl5yK/ASMaMq0Kv2W5+5b5bo1ArrpVQrM+PNLyXkhGpudoJQA87wHPZp7z1nV74t2a5LQJzZOaxq9SqwkuniGoD77TKnS15uJJPZzAKDgKVR9CuQ2/m/kVed9aqFhAISEItJVxGWhIYMjINAZSj9Gqqhq/HfImhTmS3gKG1OwP1SoNCjmCBL+D0vqerCOV7rX5eI08PCGp9Tfc48yKVbF/2Y8GWqIXpjkwbQO0zV3HunTXWs4Hs8AjLCfPsyhocgjLCG+lKKiJtvMFMi1Ktg3hFhNGBRPiVQ6uLDxprAicAolMqqaWNvD+rzUnOY9nW7KSztPcfoNHWvI="

# aws_credential = f"""[default]
# aws_access_key_id={access_id}
# aws_secret_access_key={access_key}
# aws_session_token = {session_token}
# region={region}
# """

userData= """#!/bin/bash
if [ ! -d '/home/ubuntu/CloudComputingProj1' ]
then
	cd /home/ubuntu && git clone https://github.com/Worm4047/CloudComputingProj1.git;
fi
pip install boto3;
"""

# cd /home/ubuntu/CloudComputingProj1 && python processQueue.py

def stop_instances(instance_list):
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
		KeyName="ec2-arnav",
		IamInstanceProfile={
        'Arn': 'arn:aws:iam::056594258736:instance-profile/Ec2AccessRole'
    },
		# Placement={
  #       'AvailabilityZone': 'us-east-1'
  #       },
  		SecurityGroupIds=['sg-0964b140e4c18066c'],
        DryRun=False,
        UserData=userData)
	instance_ids = []
	print(instance)
	for elem in instance['Instances']:
		if elem["Name"]!="Master":
			instance_ids.append(elem["InstanceId"])

	print(instance_ids)
	# waiter.wait(
	#     InstanceIds=instance_ids,
	#     DryRun=False
	# )
	# stop_instances(instance_ids)


create_instances(19)

