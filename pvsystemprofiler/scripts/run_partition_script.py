import sys
import os
import boto3
import time
import paramiko
import numpy as np
import pandas as pd
from modules.config_partitions import get_config
from modules.create_partition import create_partition


def get_remote_output_files(partitions, username, destination_dict):
    os.system('mkdir' + ' ' + destination_dict)
    for part_id in partitions:
        get_local_output_file = "scp -i" + part_id.ssh_key_file + ' ' + username + "@" + part_id.public_ip_address + \
                                ":" + part_id.local_output_file + ' ' + destination_dict
        os.system(get_local_output_file)


def combine_results(partitions, destination_dict):
    df = pd.DataFrame()
    for part_id in partitions:
        partial_df = pd.read_csv(destination_dict + part_id.local_output_file_name, index_col=0)
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
    global_output_directory = str(sys.argv[12])
    global_output_file = str(sys.argv[13])
    time_shift_inspection = str(sys.argv[14])

    '''
    input file location: a csv file with the system's information.
    ssh_key_file: .pem aws key file.
    aws_username: aws linux username in instances.
    aws_instance_name: aws name key used to identify instances to be used in the partitioning.
    aws_region: region as specified by aws. For example: 'us-west-1'.
    aws_client: in most cases "ec2".
    script_name: name of the script that will be run in the partitioned data.
    script_location: directory where script_name is located. 
    output_folder location: Folder where local results will be saved. This folder will be created during script execution.
    data_source: source of the data to be analyzed.
    power_column_id: id given to the power column to be analyzed.
    global_output_directory:  directory where consolidated results are saved.
    global_output_file:  name of csv file with the consolidated results.
    time_shift_inspection: indicate if manual time shift inspection should be taken into account for pipeline run.
    '''

    main_class = get_config(ifl=input_file_location, ofl=output_folder_location, skf=ssh_key_file, au=aws_username,
                            ain=aws_instance_name, ar=aws_region, ac=aws_client, ds=data_source, pcid=power_column_id,
                            gof=global_output_file, god=global_output_directory, tsi=time_shift_inspection)

    ec2_instances = get_address(aws_instance_name, aws_region, aws_client)
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
        create_partition(part)
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
        get_remote_output_files(partitions, main_class.aws_username, main_class.global_output_directory)
        results_df = combine_results(partitions, main_class.global_output_directory)
        results_df.to_csv(main_class.global_output_file)