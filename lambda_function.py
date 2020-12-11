"""Lambda function to spin up EMR cluster."""

import os
import boto3
from botocore.exceptions import ClientError
from utils import app_settings

def lambda_handler(event, context): #pylint: disable=unused-argument
    """Spin up EMR Cluster."""
    try:
        client = boto3.client(
            'emr', region_name=os.environ['AWS_DEFAULT_REGION'])
        client = boto3.client('emr')

        steps = [{
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['state-pusher-script']
            }
        }, {
            'Name': 'CSV to Parquet conversion',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    app_settings.APP_PATH,
                    app_settings.APP_INPUT,
                    app_settings.APP_OUTPUT
                ]
            }
        }]
        bootstrap_actions = []

        emr_resp =  client.run_job_flow(
            Name=app_settings.EMR_CLUSTER_NAME,
            LogUri=app_settings.APP_LOG_URI,
            ReleaseLabel=app_settings.EMR_RELEASE_LABEL,
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            EbsRootVolumeSize=int(app_settings.EBS_ROOT_VOL),
            Instances={
                'Ec2KeyName': app_settings.EC2_KEY,
                'Ec2SubnetId': app_settings.EC2_SUBNET_ID,
                'EmrManagedMasterSecurityGroup': app_settings.EMR_MASTER_SG,
                'EmrManagedSlaveSecurityGroup': app_settings.EMR_SLAVE_SG,
                'TerminationProtected': app_settings.TERMINATION_PROTECTION.lower() in ("true", 1),
                'KeepJobFlowAliveWhenNoSteps': app_settings.AUTO_TERMINATED.lower() in ("true", 0),
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': app_settings.MASTER_INS_TYPE,
                        'InstanceCount': int(app_settings.MASTER_INS_COUNT),
                        'Market': 'ON_DEMAND',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': int(app_settings.MASTER_EBS_SIZE)
                                    },
                                    'VolumesPerInstance': int(app_settings.MASTER_EBS_VOL_COUNT)
                                }
                            ]
                        }
                    },
                    {
                        'Name': 'Core',
                        'InstanceRole': 'CORE',
                        'InstanceType': app_settings.CORE_INS_TYPE,
                        'InstanceCount': int(app_settings.CORE_INS_COUNT),
                        'Market': 'SPOT',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': int(app_settings.CORE_EBS_SIZE)
                                    },
                                    'VolumesPerInstance': int(app_settings.CORE_EBS_VOL_COUNT)
                                }
                            ]
                        }
                    }
                ]
            },  # Instances
            BootstrapActions=bootstrap_actions,
            Applications=[{x.split('=')[0]: x.split('=')[1]}
                        for x in app_settings.EMR_APPS.split(' ')],
            Tags=[{'Key': x.split('=')[0], 'Value': x.split('=')[1]}
                for x in app_settings.EMR_TAGS.split(',')],
            Steps=steps
        )
    except ClientError as err:
        print(err.response['Error']['Message'])

    return emr_resp


if __name__ == "__main__":
    lambda_handler({}, '')
