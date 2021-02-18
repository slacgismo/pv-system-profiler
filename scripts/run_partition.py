import os
import boto3
import paramiko


def create_partition(partition, i, instance):
    python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
    start_index = partition.ix_0
    end_index = partition.ix_n
    input_file = partition.input_file_location
    output_file = partition.output_file
    output_folder_location = partition.output_folder_location
    output_folder = partition.output_folder
    script_location = partition.script_location
    local_script = script_location + 'local_script.py'
    run_script = script_location + 'longitude_script.py'
    ssh_username = partition.aws_username
    ssh_key_file = partition.ssh_key_file

    #print(local_script)
    #print(output_folder_location)
    #scp_command = 'scp -i ' + ssh_key_file + ' local_script.py ubuntu@' + instance + ':/home/ubuntu'
    #os.system(scp_command)
    commands = ['mkdir -p' + ' ' + output_folder_location + output_folder + 'data']
    #            python + ' ' + 'start_index' + ' ' + 'end_index' + local_script + ' ' + input_file + ' ' + output_file]
    #commands = [python + ' ' + local_script.py' + ' ' + location + ' ' +
     #           str(ii) + ' ' + str(jj) + ' ' + str(i) + str(' ') + str(n_chunks)]

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
