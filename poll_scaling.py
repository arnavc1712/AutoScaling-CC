import boto3
import math
import paramiko
import threading


import sched, time

ec2_client = boto3.client('ec2',region_name="us-east-1")

sqs_client = boto3.client('sqs',region_name="us-east-1")

sqs_url = 'https://sqs.us-east-1.amazonaws.com/056594258736/video-process'

waiter_run = ec2_client.get_waiter('instance_running')
waiter_stop = ec2_client.get_waiter('instance_stopped')



def slave_thread(host,inst_id,msg_body,msg_receipt_handle):
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	print('Connecting to ' + host)
	time.sleep(5)
	while True:
		try:
			ssh.connect(host, username='ubuntu', key_filename='ec2-arnav.pem')
			print('Connected to ' + host)
			break
		except Exception as err:
			print(err)
	
	stdin, stdout, stderr = ssh.exec_command(f"pip install boto3;Xvfb :1 & export DISPLAY=:1;cd /home/ubuntu/CloudComputingProj1 && sudo git pull origin master;cd /home/ubuntu/CloudComputingProj1 && sudo python processQueue.py {msg_body} {msg_receipt_handle};")
	print(stdout.read())
	print(stderr.read())

	print(f"Output of {host}\n")

	stdout.close()
	stdin.close()
	ssh.close()

	stop_instances([inst_id])



def handle_visibility(queue_url, reciept_handle, value):
    logging.info("Handing visibility for ", reciept_handle)
    try:
        response = sqs_client.change_message_visibility(
            QueueUrl= queue_url,
            ReceiptHandle=reciept_handle,
            VisibilityTimeout=value
        )
        print(response)
    except Exception as e:
        print(e)

def scale_up_instances(instance_id,message):
	max_number_tries = 10
	while max_number_tries>0:

		try:
			start_instances([instance_id])
			waiter_run.wait(
		    InstanceIds=[instance_id],
		    DryRun=False
			)
			break
		except Exception as e:
			print(e)

		time.sleep(3)
		max_number_tries-=1

	

	instance_dns_names,instance_ids = get_inst_dns_names([instance_id])
	inst_dns = instance_dns_names[0]
	inst_id = instance_ids[0]

	msg_body = message["Body"]
	msg_receipt_handle = message["ReceiptHandle"]

	if max_number_tries==0:
		handle_visibility(sqs_url,msg_receipt_handle,0)
		return

	slave_thread(inst_dns,inst_id,str(msg_body),str(msg_receipt_handle))



def start_instances(instance_list):
	try:
		response = ec2_client.start_instances(
		    InstanceIds=instance_list,
		    DryRun=False
		)


		print(response)
	
	except Exception as err:
		print(err)


def stop_instances(instance_list):
	try:
		response = ec2_client.stop_instances(
		    InstanceIds=instance_list,
		    DryRun=False
		)

		print(response)
	
	except Exception as err:
		print(err)

def fetch_instances(status=None,instance_ids=None):
	if instance_ids == None:
		response = ec2_client.describe_instances(
			DryRun=False)
	else:
		response = ec2_client.describe_instances(
		InstanceIds=instance_ids,DryRun=False)
	# print(response)
	instances = []
	for elem in response['Reservations']:
		for instance in elem["Instances"]:
			if instance["InstanceId"]!="i-06389036ae7bedc1d":
				if status != None and instance["State"]["Name"]==status:
					instances.append(instance)
				elif instance_ids!=None:
					instances.append(instance)
			# 	print(instance["PublicDnsName"])
			# 	instance_list.append(instance["InstanceId"])

	return instances


def get_inst_dns_names(instance_ids):
	instances = fetch_instances(instance_ids=instance_ids)

	dns_names = []
	instance_ids = []
	for instance in instances:
		dns_names.append(instance["PublicDnsName"])
		instance_ids.append(instance["InstanceId"])


	return dns_names,instance_ids





def get_instance_ids(status):
	instances = fetch_instances(status=status)
	instance_ids = []
	for instance in instances:
		instance_ids.append(instance["InstanceId"])

	return instance_ids

		



def get_queue_attributes():
	response = sqs_client.get_queue_attributes(
    QueueUrl=sqs_url,
    AttributeNames=[
        'ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible'
    ]
	)

	return response



def poll_for_scaling():
	queue_attr = get_queue_attributes()["Attributes"]
	num_messages_queue = int(queue_attr["ApproximateNumberOfMessages"])
	acceptable_backlog = 1
	
	stopped_states = get_instance_ids("stopped")
	stopping_states = get_instance_ids("stopping")
	instance_running = get_instance_ids("running") + get_instance_ids("pending")


	instance_stopped = get_instance_ids("stopped") + get_instance_ids("stopping")


	instance_terminated = get_instance_ids("terminated") + get_instance_ids("shutting-down")

	max_instance_limit = len(instance_running) + len(instance_stopped)

	num_instances_needed = math.ceil(num_messages_queue/float(acceptable_backlog))


	print("Number of instances Running: ",len(instance_running))
	print("Number of instances Stopped: ",len(instance_stopped))
	print("Number of instances Temrinated: ",len(instance_terminated))

	print("Number of messages in queue: ",num_messages_queue)

	print("Number of instances needed: ",num_instances_needed)

	print("\n\n\n")
	

	if num_instances_needed>0:
		if len(stopped_states)<num_instances_needed and len(stopping_states)>0: ## If there are enough stopped states, do not scale, otherwise wait for instances which are stopping
			waiter_stop.wait(
			    InstanceIds=stopping_states,
			    DryRun=False
			)
			stopped_states = stopped_states+stopping_states

		max_extra_instances = max_instance_limit - len(instance_running) ## Maximum extra instances we can add
		print(f"Max Extra instances: {max_extra_instances}")
		
		if max_extra_instances==0:
			print("We have reached our limit. Cannot scale up")
	
		else:
			print("Scaling UP by ",min(num_instances_needed,max_extra_instances))
			for i in range(min(num_instances_needed,max_extra_instances)):
				queue = sqs_client.receive_message(QueueUrl=sqs_url,VisibilityTimeout=700)
				if "Messages" in queue:
					t1=threading.Thread(target=scale_up_instances,args=(stopped_states[i],queue["Messages"][0]))
					t1.start()
				else:
					break

while True:
	poll_for_scaling()
	time.sleep(10)


