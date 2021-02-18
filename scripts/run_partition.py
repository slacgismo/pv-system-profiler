import config
import json
import pandas as pd
import os
import boto3
import paramiko

def run_config(partition, i,  instance):
    python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
    ii = partition.ix_0
    jj = partition.ix_n
    site_list_file = partition.site_list_file
    location = partition.location
    chunk_id = partition.chunk_id
    n_chunks =  partition.n_chunks
    ssh_username = partition.ssh_username
    ssh_key_file = partition.ssh_key_file
    report_file = partition.site_report_file
    scp_command = 'scp -i ' + ssh_key_file + ' local_script.py ubuntu@' + instance + ':/home/ubuntu'
    os.system(scp_command)  
    commands = [python + ' ' + 'local_script.py' + ' ' +  location + ' ' + 
            str(ii) + ' ' + str(jj) + ' ' +str(i) + str(' ') + str(n_chunks)]
   
    k = paramiko.RSAKey.from_private_key_file(ssh_key_file)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=instance, username=ssh_username, pkey=k, allow_agent=False, look_for_keys=False)
    for command in commands:
            print("running command: {}".format(command))
            stdin , stdout, stderr = c.exec_command(command)
            print(stdout.read())
            print(stderr.read())
    c.close()

def get_address(ssh_username, ssh_key_file, tag_name, region, client):
    ec2 = boto3.Session(profile_name='default', region_name=region).client(client)
    target_instances = ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':[tag_name]}])

    ec2_instances = []
    for each_instance in target_instances['Reservations']:
        for found_instance in each_instance['Instances']:
            if found_instance['PublicDnsName'] != '':
                ec2_instances.append(found_instance['PublicDnsName'])
    return ec2_instances
