"""Lambda function to spin up EMR cluster."""

import os
import boto3
from botocore.exceptions import ClientError


def launch_emr_cluster(args):
    """Spin up EMR cluster."""
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
                    '--class', 'overwrite',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    '{} -i {} -o {}'.format(
                        args['app_path'],
                        args['app_input'],
                        args['app_output']
                    )
                ]
            }
        }]
        bootstrap_actions = []

        return client.run_job_flow(
            Name=args['emr_cluster_name'],
            LogUri=args['app_log_uri'],
            ReleaseLabel=args['emr_release_label'],
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            EbsRootVolumeSize=int(args['ebs_root_vol']),
            Instances={
                'Ec2KeyName': args['ec2_key'],
                'Ec2SubnetId': args['ec2_subnet_id'],
                'EmrManagedMasterSecurityGroup': args['emr_master_sg'],
                'EmrManagedSlaveSecurityGroup': args['emr_slave_sg'],
                'TerminationProtected': args['termination_protection'].lower() in ("true", 1),
                'KeepJobFlowAliveWhenNoSteps': args['auto_terminated'].lower() in ("true", 0),
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': args['emr_master_ins_type'],
                        'InstanceCount': int(args['emr_master_ins_count']),
                        'Market': 'ON_DEMAND',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': int(args['emr_master_ebs_size'])
                                    },
                                    'VolumesPerInstance': int(args['emr_master_ebs_vol_count'])
                                }
                            ]
                        }
                    },
                    {
                        'Name': 'Core',
                        'InstanceRole': 'CORE',
                        'InstanceType': args['emr_core_ins_type'],
                        'InstanceCount': int(args['emr_core_ins_count']),
                        'Market': 'SPOT',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': int(args['emr_core_ebs_size'])
                                    },
                                    'VolumesPerInstance': int(args['emr_core_ebs_vol_count'])
                                }
                            ]
                        }
                    }
                ]
            },  # Instances
            BootstrapActions=bootstrap_actions,
            Applications=[{x.split('=')[0]: x.split('=')[1]}
                        for x in args['emr_apps'].split(' ')],
            Tags=[{'Key': x.split('=')[0], 'Value': x.split('=')[1]}
                for x in args['emr_tags'].split(',')],
            Steps=steps
        )
    except ClientError as err:
        print(err.response['Error']['Message'])


def lambda_handler(event, context): #pylint: disable=unused-argument
    """lalalala."""
    emr_args = {
        'app_path': os.environ['APP_PATH'],
        'app_input': os.environ['APP_INPUT'],
        'app_output': os.environ['APP_OUTPUT'],
        'app_log_uri': os.environ['APP_LOG_URI'],
        'emr_cluster_name': os.environ['EMR_CLUSTER_NAME'],
        'emr_release_label': os.environ['EMR_RELEASE_LABEL'],
        'emr_apps': os.environ['EMR_APPS'],
        'emr_tags': os.environ['EMR_TAGS'],
        'ebs_root_vol': os.environ['EBS_ROOT_VOL'],
        'ec2_key': os.environ['EC2_KEY'],
        'ec2_subnet_id': os.environ['SUBNET_ID'],
        'emr_master_sg': os.environ['EMR_MASTER_SG'],
        'emr_slave_sg': os.environ['EMR_SLAVE_SG'],
        'emr_master_ins_type': os.environ['MASTER_INS_TYPE'],
        'emr_master_ins_count': os.environ['MASTER_INS_COUNT'],
        'emr_master_ebs_size': os.environ['MASTER_EBS_SIZE'],
        'emr_master_ebs_vol_count': os.environ['MASTER_EBS_VOL_COUNT'],
        'emr_core_ins_type': os.environ['CORE_INS_TYPE'],
        'emr_core_ins_count': os.environ['CORE_INS_COUNT'],
        'emr_core_ebs_size': os.environ['CORE_EBS_SIZE'],
        'emr_core_ebs_vol_count': os.environ['CORE_EBS_VOL_COUNT'],
        'termination_protection': os.environ['TERMINATION_PROTECTION'],
        'auto_terminated': os.environ['AUTO_TERMINATED']
    }
    emr_resp = launch_emr_cluster(emr_args)
    print(emr_resp)


if __name__ == "__main__":
    lambda_handler({}, '')
