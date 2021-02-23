import sys
import os
import boto3
import time
import paramiko
import numpy as np
import pandas as pd
from modules.config_partitions import get_config
from modules.create_partition import create_partition


def get_remote_output_files(partitions):
    for part_id in partitions:
        get_local_output_file = "scp -i" + " '" + "~/.aws/londonoh.pem" + "' " + "ubuntu@" + \
                                part_id.public_ip_address + ":" + part_id.local_output_file + " ./"
        os.system(get_local_output_file)

def combine_results(partitions):
    df = pd.DataFrame()
    for part_id in partitions:
        partial_df = pd.read_csv(part.local_output_file_name, index_col=0)
        df = df.append(partial_df, ignore_index=True)
        df.index = np.arange(len(df))
    return df


def check_completion(ssh_username, instance_id, ssh_key_file):
    commands = ["grep 'finished' ./out"]
    commands_dict = remote_execute(user=ssh_username, instance_id=instance, key=ssh_key_file, shell_commands=commands)
    if str(commands_dict["grep 'finished' ./out"][0]).find('finished') != -1:
        return True
    else:
        return False


def get_address(tag_name, region, client):
    ec2 = boto3.Session(profile_name='default', region_name=region).client(client)
    target_instances = ec2.describe_instances(Filters=[{'Name': 'tag:Name', 'Values': [tag_name]}])

    ec2_instances = []
    for each_instance in target_instances['Reservations']:
        for found_instance in each_instance['Instances']:
            if found_instance['PublicDnsName'] != '':
                ec2_instances.append(found_instance['PublicDnsName'])
    return ec2_instances


def remote_execute(user, instance_id, key, shell_commands):
    k = paramiko.RSAKey.from_private_key_file(key)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=instance_id, username=user, pkey=k, allow_agent=False, look_for_keys=False)
    command_dict = {}
    for command in shell_commands:
        print("running command: {}".format(command))
        stdin, stdout, stderr = c.exec_command(command)
        command_dict[command] = [stdout.read(), stderr.read()]

    c.close()
    return command_dict


if __name__ == '__main__':
    input_file_location = str(sys.argv[1])
    ssh_key_file = str(sys.argv[2])
    aws_username = str(sys.argv[3])
    aws_instance_name = str(sys.argv[4])
    aws_region = str(sys.argv[5])
    aws_client = str(sys.argv[6])
    script_name = str(sys.argv[7])
    script_location = str(sys.argv[8])
    output_folder_location = str(sys.argv[9])
    data_source = str(sys.argv[10])
    power_column_id = str(sys.argv[11])
    global_output_file = str(sys.argv[12])
    time_shift_inspection = str(sys.argv[13])

    main_class = get_config(ifl=input_file_location, ofl=output_folder_location, skf=ssh_key_file, au=aws_username,
                            ain=aws_instance_name, ar=aws_region, ac=aws_client, ds=data_source, pcid=power_column_id,
                            gof=global_output_file, tsi=time_shift_inspection)

    ec2_instances = get_address(aws_instance_name, aws_region, aws_client)
    print(ec2_instances)
    df = pd.read_csv(input_file_location, index_col=0)
    n_part = len(ec2_instances)
    ll = len(df) - 1
    part_size = int(ll / n_part) + 1
    i = 0
    jj = 0
    partitions = []
    processes = []
    while jj < ll:
        ii = i * part_size
        jj = part_size * (i + 1)
        if jj >= ll:
            jj = ll
        part = get_config(part_id=i, ix_0=ii, ix_n=ii + 3, n_part=n_part, ifl=input_file_location,
                          ofl=output_folder_location, ip_address=ec2_instances[i], skf=ssh_key_file, au=aws_username,
                          ain=aws_instance_name, ar=aws_region, ac=aws_client, script_name=script_name,
                          scripts_location=script_location, ds=data_source, pcid=power_column_id,
                          tsi=time_shift_inspection)

        partitions.append(part)

        create_partition(part, i)
        i += 1

    process_completed = False

    while not process_completed:
        for part_ix, part_id in enumerate(partitions):
            if part.process_completed is False:
                process_completed = True
                ssh_key_file = part_id.ssh_key_file
                instance = part_id.public_ip_address
                ssh_username = part_id.aws_username
                part_id.process_completed = check_completion(ssh_username, instance, ssh_key_file)
                process_completed = process_completed & part_id.process_completed

        time.sleep(60)

    if process_completed:
        get_remote_output_files(partitions)
        results_df = combine_results(partitions)
        results_df.to_csv(global_output_file)



