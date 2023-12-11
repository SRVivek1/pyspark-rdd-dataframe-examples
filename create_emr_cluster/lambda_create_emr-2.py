import boto3

"""
    EMR default profile references.
        --> https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html

    Create non-existing EMR default roles
        --> https://repost.aws/knowledge-center/emr-default-role-invalid

        Run below command on cloud-shell
            --> aws emr create-default-roles
"""


def lambda_handler(event, context):
    client = boto3.client('emr', region_name="us-east-2")

    instances = {
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 2,
        'InstanceGroups': [],
        'Ec2KeyName': 'ec2-putty-ppk-key-pair',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-023701fa5258f4097',
        'EmrManagedMasterSecurityGroup': 'sg-013af8716ec4df1a5',
        'EmrManagedSlaveSecurityGroup': 'sg-0831f5f387597c14c'
    }

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

    response = client.run_job_flow(
        Name='PySpark-Cluster-boto3',
        LogUri='s3://vsingh-dev.spark.test.data/emr-cluster-logs',
        ReleaseLabel='emr-6.15.0',
        Instances=instances,
        Configurations=configurations,
        Steps=[],
        BootstrapActions=[],
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Zeppelin'},
            {'Name': 'Ganglia'}
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        EbsRootVolumeSize=30
    )
    return response["JobFlowId"]

