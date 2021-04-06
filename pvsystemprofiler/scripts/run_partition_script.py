import sys
import os
import boto3
import time
import math
import numpy as np
import pandas as pd
from modules.config_partitions import get_config
from modules.create_partition import create_partition
from modules.script_functions import enumerate_files
from modules.script_functions import get_s3_bucket_and_prefix
from modules.script_functions import copy_to_s3
from modules.script_functions import log_file_versions
from modules.script_functions import remote_execute


def build_input_file(s3_location, input_file_location):
    bucket, prefix = get_s3_bucket_and_prefix(s3_location)
    site_list, size_list = enumerate_files(bucket, prefix, file_size_list=True)
    site_df = pd.DataFrame()
    site_df['site'] = site_list
    site_df['site'] = site_df['site'].apply(lambda x: x.split('.')[0])
    site_df['file_size'] = size_list
    site_df.to_csv('./generated_site_list.csv')
    bucket, prefix = get_s3_bucket_and_prefix(input_file_location)
    copy_to_s3('./generated_site_list.csv', bucket, prefix)
    return site_df

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
    print(instance_id)
    commands = ["grep -a '%' ./out", "ps -x |grep -o 'python'"]
    commands_dict = remote_execute(user=ssh_username, instance_id=instance_id, key=ssh_key_file,
                                   shell_commands=commands, verbose=False)
    for command_i in commands_dict.keys():
        print(commands_dict[command_i][0])

    commands = ["grep -a 'finished' ./out"]
    commands_dict = remote_execute(user=ssh_username, instance_id=instance_id, key=ssh_key_file,
                                   shell_commands=commands, verbose=False)
    if str(commands_dict["grep -a 'finished' ./out"][0]).find('finished') != -1:
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


def main(df, ec2_instances, input_file_location, output_folder_location, ssh_key_file, aws_username, aws_instance_name,
         aws_region, aws_client, script_name, script_location, power_column_id, time_shift_inspection,
         s3_location, n_files, file_label, fix_time_shifts, time_zone_correction, check_json, supplementary_file):
    n_part = len(ec2_instances)
    total_size = np.sum(df['file_size'])
    part_size = np.ceil(total_size / n_part) * 0.8
    ii = 0
    jj = 0
    partitions = []
    for i in range(n_part):
        local_size = 0
        while local_size < part_size:
            local_size = jj - ii
            if i == n_part - 1:
                jj = len(df) - 1
                local_size = part_size + 1
            local_size = np.sum(df.loc[ii:jj, 'file_size'])
            jj += 1

        part = get_config(part_id=i, ix_0=ii, ix_n=jj, n_part=n_part, ifl=input_file_location,
                          ofl=output_folder_location, ip_address=ec2_instances[i], skf=ssh_key_file, au=aws_username,
                          ain=aws_instance_name, ar=aws_region, ac=aws_client, script_name=script_name,
                          scripts_location=script_location, pcid=power_column_id, tsi=time_shift_inspection,
                          s3l=s3_location, n_files=n_files, file_label=file_label, fix_time_shifts=fix_time_shifts,
                          time_zone_correction=time_zone_correction, check_json=check_json, sup_file=supplementary_file)
        partitions.append(part)
        create_partition(part)
        ii = jj + 1
        jj = ii

    completion = [False] * len(partitions)
    while False in completion:
        for part_ix, part_id in enumerate(partitions):
            if completion[part_ix] is False:
                print('Partition' + ' ', part_ix)
                ssh_key_file = part_id.ssh_key_file
                instance = part_id.public_ip_address
                ssh_username = part_id.aws_username
                new_value = check_completion(ssh_username, instance, ssh_key_file)
                part_id.process_completed = new_value
                completion[part_ix] = new_value
        time.sleep(10 * 60)

    get_remote_output_files(partitions, main_class.aws_username, main_class.global_output_directory)
    results_df = combine_results(partitions, main_class.global_output_directory)
    results_df.to_csv(main_class.global_output_file)
    return


if __name__ == '__main__':
    create_input_file = str(sys.argv[1])
    input_file_location = str(sys.argv[2])
    ssh_key_file = str(sys.argv[3])
    aws_username = str(sys.argv[4])
    aws_instance_name = str(sys.argv[5])
    aws_region = str(sys.argv[6])
    aws_client = str(sys.argv[7])
    script_name = str(sys.argv[8])
    script_location = str(sys.argv[9])
    output_folder_location = str(sys.argv[10])
    power_column_id = str(sys.argv[11])
    global_output_directory = str(sys.argv[12])
    global_output_file = str(sys.argv[13])
    s3_location = str(sys.argv[14])
    n_files = str(sys.argv[15])
    file_label = str(sys.argv[16])
    time_shift_inspection = str(sys.argv[17])
    fix_time_shifts = str(sys.argv[18])
    time_zone_correction = str(sys.argv[19])
    check_json = str(sys.argv[20])
    supplementary_file = str(sys.argv[21])
    '''
    :create_input_file: True if a csv file with the system's information to be generated. False if file provided.
    :input file location: The csv file with the system's information. If create_input_file=True a file with the 
    specified name and location is created.
    :ssh_key_file: .pem aws key file.
    :aws_username: aws linux username in instances.
    :aws_instance_name: aws name key used to identify instances to be used in the partitioning.
    :aws_region: region as specified by aws. For example: 'us-west-1'.
    :aws_client: in most cases "ec2".
    :script_name: name of the script that will be run partitioned.
    :script_location: full path to directory where script_name is located. 
    :output_folder_location: Full path to folder where local results will be saved. This folder will be created during 
    script execution.
    :power_column_id: id given to the power column to be analyzed.
    :global_output_directory:  directory where consolidated results are saved.
    :global_output_file:  name of csv file with the consolidated results.
    :time_shift_inspection: indicate if manual time shift inspection should be taken into account for pipeline run.
    :s3_location: Absolute path to s3 location of files with power data.
    :n_files: number of files to read. If 'all' all files in folder are read.
    :file_label:  Repeating portion of data files label. If 'None', no file label is used. 
    :fix_time_shifts: String, 'True' or 'False', determines if time shifts are fixed when running the pipeline
    :time_zone_correction: String, 'True' or 'False', determines if time zone correction is performed when running  the
    pipeline
    :check_json: String, 'True' or 'False'. Check json file for location information. 
    :supplementary_file: csv file with supplementary information need to run script.
    '''
    # log_file_versions('solar_data_tools')
    if create_input_file == 'True':
        build_input_file(s3_location, input_file_location)

    main_class = get_config(ifl=input_file_location, ofl=output_folder_location, skf=ssh_key_file, au=aws_username,
                            ain=aws_instance_name, ar=aws_region, ac=aws_client, pcid=power_column_id,
                            gof=global_output_file, god=global_output_directory, tsi=time_shift_inspection,
                            s3l=s3_location, n_files=n_files, file_label=file_label, fix_time_shifts=fix_time_shifts,
                            time_zone_correction=time_zone_correction, check_json=check_json,
                            sup_file=supplementary_file)

    ec2_instances = get_address(aws_instance_name, aws_region, aws_client)
    df = pd.read_csv(input_file_location, index_col=0)

    main(df, ec2_instances, input_file_location, output_folder_location, ssh_key_file, aws_username, aws_instance_name,
         aws_region, aws_client, script_name, script_location, power_column_id, time_shift_inspection, s3_location,
         n_files, file_label, fix_time_shifts, time_zone_correction, check_json,supplementary_file)
