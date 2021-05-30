"""
This script is used to create AWS EMR cluster using AWS Lambda script.

boto3
    > boto3 library is used to communicate with AWS services to automate the creation process.
"""


import boto3


def lambda_handler(event, context):

    # Define the EMR instance definition
    emr_instances = {
        'MasterInstanceType': 'm3.xlarge',
        'SlaveInstanceType': 'm3.xlarge',
        'InstanceCount': 2,
        'InstanceGroups': [],
        'Ec2KeyName': 'ec2-pem-1',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-b09f76fb',
        'EmrManagedMasterSecurityGroup': 'sg-075384ab24b3274d8',
        'EmrManagedSlaveSecurityGroup':  'sg-00de6c40244eef937'
    }

    # Configurations for YARN Cluster & Python path.
    configurations = [
        {
            'Classification': 'yarn-site',
            'Properties': {
                'yarn.resourcemanager.scheduler.class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler'
            },
            'Configurations': []
        },
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ]

    # Create aws client using boto3 library
    emr_client = boto3.client('emr', region_name='eu-west-1')

    response = emr_client.run_job_flow(
        Name='PySpark Cluster',
        LogUri='s3://test-emr-logs-123/emr-logs',
        ReleaseLabel='emr-5.30.0',
        Instances=emr_instances,
        Configurations=configurations,
        Steps=[],
        BootstrapActions=[],
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Zeppelin'},
            {'Name': 'Ganglia'}
        ],
        VisibleToAllUsers=True,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        EbsRootVolumeSize=30
    )

    # Return JobFlowId from Response
    return response["JobFlowId"]
