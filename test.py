
import boto3
sqs_client = boto3.client('sqs')
autoscaling_client = boto3.client('autoscaling')
cloudwatch_client = boto3.client('cloudwatch')

# for queue in sqs.queues.all():
#     print(queue.url)
# queue = sqs.get_queue_by_name(QueueName='Video-processing-std')

# for i in range(100):
# 	response = sqs_client.send_message(
#     QueueUrl='https://sqs.us-east-1.amazonaws.com/056594258736/Video-processing-std',
#     MessageBody='Wazzzuppppp')
# 	print(response)

response = sqs_client.get_queue_attributes(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/056594258736/Video-processing-std",
    AttributeNames=[
        'All'
    ]
)

apx_no_of_messages = response["Attributes"]["ApproximateNumberOfMessages"]


response = autoscaling_client.describe_auto_scaling_groups(
    AutoScalingGroupNames=[
        'Cloud_Computing_P1'
    ],

    MaxRecords=100
)

fleet_list = response["AutoScalingGroups"][0]["Instances"]

fleet_capacity = len(list(filter(lambda x: x["LifecycleState"]=="InService",fleet_list)))


response = cloudwatch_client.put_metric_data(
    Namespace='MyNamespace',
    MetricData=[
        {
            'MetricName': 'BacklogPerInstance',
            'Value': int(apx_no_of_messages)//int(fleet_capacity),
            'Unit': 'Count',
            'StorageResolution': 60
        },
    ]
)

print(response)





