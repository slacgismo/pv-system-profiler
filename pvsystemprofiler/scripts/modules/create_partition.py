import paramiko


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
    data_source = partition.data_source
    power_column_id = partition.power_column_id
    time_shift_inspection = partition.time_shift_inspection

    commands = ['rm estimation* -rf',
                'mkdir -p' + ' ' + local_working_folder + 'data',
                python + ' ' + local_script + ' '
                + str(start_index) + ' '
                + str(end_index) + ' '
                + script_name + ' '
                + global_input_file + ' '
                + local_input_file + ' '
                + local_output_file + ' '
                + data_source + ' '
                + power_column_id + ' '
                + time_shift_inspection]

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
