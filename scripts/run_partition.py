import os
import boto3
import paramiko


def create_partition(partition, i, instance):
    python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
    start_index = partition.ix_0
    end_index = partition.ix_n
    global_input_file = partition.input_file_location
    local_input_file = partition.local_input_file
    local_output_file = partition.local_output_file
    local_output_folder = partition.local_output_folder
    local_folder_location = partition.local_output_folder_location
    script_location = partition.script_location
    local_script = script_location + 'local_script.py'
    run_script = script_location + 'longitude_script.py'
    ssh_username = partition.aws_username
    ssh_key_file = partition.ssh_key_file

    #scp_command = 'scp -i ' + ssh_key_file + ' local_script.py ubuntu@' + instance + ':/home/ubuntu'
    #os.system(scp_command)
    commands = ['rm out* -rf',
                 'mkdir -p' + ' ' + local_folder_location + local_output_folder + 'data',
                 python + ' ' + local_script + ' ' + str(start_index) + ' ' + str(end_index) + ' ' + global_input_file +
                 ' ' + local_output_folder + 'data/' + local_input_file + ' ' + local_output_file]
    # print(commands)

    k = paramiko.RSAKey.from_private_key_file(ssh_key_file)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=instance, username=ssh_username, pkey=k, allow_agent=False, look_for_keys=False)
    for command in commands:
        print("running command: {}".format(command))
        stdin, stdout, stderr = c.exec_command(command)
        print(stdout.read())
        print(stderr.read())
    c.close()


def get_address(ssh_username, ssh_key_file, tag_name, region, client):
    ec2 = boto3.Session(profile_name='default', region_name=region).client(client)
    target_instances = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [tag_name]}])

    ec2_instances = []
    for each_instance in target_instances['Reservations']:
        for found_instance in each_instance['Instances']:
            if found_instance['PublicDnsName'] != '':
                ec2_instances.append(found_instance['PublicDnsName'])
    return ec2_instances
