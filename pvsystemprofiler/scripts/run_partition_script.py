import sys
import os
import boto3
import time
import glob
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


def build_input_file(s3_location, input_file_location='s3://pv.insight.misc/report_files/'):
    bucket, prefix = get_s3_bucket_and_prefix(s3_location)
    site_list, size_list = enumerate_files(bucket, prefix, file_size_list=True)
    site_df = pd.DataFrame()
    site_df['site'] = site_list
    site_df['site'] = site_df['site'].apply(lambda x: x.split('.')[0])
    site_df['file_size'] = size_list
    site_df.to_csv('./generated_site_list.csv')
    bucket, prefix = get_s3_bucket_and_prefix(input_file_location)
    copy_to_s3('./generated_site_list.csv', bucket, prefix + '/generated_site_list.csv')
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


def main(df, ec2_instances, site_input_file, output_folder_location, ssh_key_file, aws_username, aws_instance_name,
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
            local_size = np.sum(df.loc[ii:jj, 'file_size'])
            if i == n_part - 1:
                jj = len(df) - 1
                local_size = part_size + 1
            jj += 1

        part = get_config(part_id=i, ix_0=ii, ix_n=jj, n_part=n_part, ifl=site_input_file,
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
    input_site_file = str(sys.argv[1])
    s3_location = str(sys.argv[2])
    aws_instance_name = str(sys.argv[3])
    script_to_execute = str(sys.argv[4])
    power_column_id = str(sys.argv[5])
    n_files = str(sys.argv[6])
    file_label = str(sys.argv[7])
    time_shift_inspection = str(sys.argv[8])
    fix_time_shifts = str(sys.argv[9])
    time_zone_correction = str(sys.argv[10])
    check_json = str(sys.argv[11])
    supplementary_file = str(sys.argv[12])
    '''
    :param input_site_file: Absolute path to csv file containing a list of sites to be evaluated. 'None' if no input 
    site file is provided.
    :param s3_location: Absolute path to s3 location of csv files containing site power signal time series.
    :param aws_instance_name: aws name key used to identify instances to be used in the partitioning.
    :param Full path to python script to be executed.
    :param power_column_id: id given to the power column to be analyzed.
    :param time_shift_inspection: indicate if manual time shift inspection should be taken into account for pipeline
     run.
    :param n_files: number of files to read. If 'all' all files in folder are read.
    :param file_label:  Repeating portion of data files label. If 'None', no file label is used. 
    :param fix_time_shifts: String, 'True' or 'False', determines if time shifts are fixed when running the pipeline.
    :param time_zone_correction: String, 'True' or 'False', determines if time zone correction is performed when running 
    the pipeline
    :param check_json: String, 'True' or 'False'. Check json file for location information. 
    :param supplementary_file: csv file with supplementary information need to run script.
    '''
    # log_file_versions('solar_data_tools')

    if input_site_file == 'None':
        build_input_file(s3_location)
        site_input_file = 's3://pv.insight.misc/report_files/generated_site_list.csv'
    aws_username = 'ubuntu'
    aws_region = 'us-west-1'
    aws_client = 'ec2'
    output_folder_location = '~/'
    global_output_directory = '~/results/'
    global_output_file = 'results.csv'
    pos = script_to_execute.rfind('/') + 1
    script_location = script_to_execute[:pos]
    script_name = script_to_execute.split('/')[-1]

    try:
        ssh_key_file = glob.glob("/Users/*/.aws/*.pem")[0]
    except:
        ssh_key_file = glob.glob("/home/*/.aws/*.pem")[0]

    main_class = get_config(ifl=site_input_file, ofl=output_folder_location, skf=ssh_key_file, au=aws_username,
                            ain=aws_instance_name, ar=aws_region, ac=aws_client, pcid=power_column_id,
                            gof=global_output_file, god=global_output_directory, tsi=time_shift_inspection,
                            s3l=s3_location, n_files=n_files, file_label=file_label, fix_time_shifts=fix_time_shifts,
                            time_zone_correction=time_zone_correction, check_json=check_json,
                            sup_file=supplementary_file)

    ec2_instances = get_address(aws_instance_name, aws_region, aws_client)
    df = pd.read_csv(site_input_file, index_col=0)

    main(df, ec2_instances, site_input_file, output_folder_location, ssh_key_file, aws_username, aws_instance_name,
         aws_region, aws_client, script_name, script_location, power_column_id, time_shift_inspection, s3_location,
         n_files, file_label, fix_time_shifts, time_zone_correction, check_json, supplementary_file)
