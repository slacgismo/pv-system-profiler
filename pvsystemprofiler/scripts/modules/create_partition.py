import paramiko
from modules.script_functions import remote_execute

def create_partition(partition):
    python = '/home/ubuntu/miniconda3/envs/pvi-dev/bin/python'
    start_index = partition.ix_0
    end_index = partition.ix_n
    global_input_file = partition.input_file_location
    local_input_file = partition.local_input_file
    local_working_folder = partition.local_working_folder
    local_output_file = partition.local_output_file
    script_name = partition.script_name
    scripts_location = partition.scripts_location
    local_script = scripts_location + 'modules/local_partition_script.py'
    instance = partition.public_ip_address
    ssh_username = partition.aws_username
    ssh_key_file = partition.ssh_key_file
    power_column_id = partition.power_column_id
    time_shift_inspection = partition.time_shift_inspection
    s3_location = partition.s3_location
    n_files = partition.n_files
    file_label = partition.file_label
    fix_time_shifts = partition.fix_time_shifts
    time_zone_correction = partition.time_zone_correction
    check_json = partition.check_json
    supplementary_file = partition.supplementary_file

    commands = ['rm estimation* -rf']
    output = remote_execute(ssh_username, instance, ssh_key_file, commands)

    commands = ['ls' + ' ' + local_working_folder + 'd']
    output = remote_execute(ssh_username, instance, ssh_key_file, commands)
    if str(output[commands[0]][1]).find('No such file or directory'):
        commands = ['rm estimation* -rf',
                    'mkdir -p' + ' ' + local_working_folder + 'data',
                    python + ' ' + local_script + ' '
                    + str(start_index) + ' '
                    + str(end_index) + ' '
                    + script_name + ' '
                    + global_input_file + ' '
                    + local_input_file + ' '
                    + local_output_file + ' '
                    + power_column_id + ' '
                    + time_shift_inspection + ' '
                    + s3_location + ' '
                    + n_files + ' '
                    + file_label + ' '
                    + fix_time_shifts + ' '
                    + time_zone_correction + ' '
                    + check_json + ' '
                    + supplementary_file
                    ]

    else:
        commands = [local_input_file.split('data')[0] + 'run_local_partition.sh']

    remote_execute(ssh_username, instance, ssh_key_file, commands)
