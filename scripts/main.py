import sys
import pandas as pd
from config import get_config
from run_partition import get_address
from run_partition import create_partition

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
    time_shift_inspection = str(sys.argv[12])

    main_class = get_config(ifl=input_file_location, ofl=output_folder_location, skf=ssh_key_file, au=aws_username,
                            ain=aws_instance_name, ar=aws_region, ac=aws_client, ds=data_source, pcid=power_column_id,
                            tsi=time_shift_inspection)

    ec2_instances = get_address(aws_username, ssh_key_file, aws_instance_name, aws_region, aws_client)

    df = pd.read_csv(input_file_location, index_col=0)
    n_part = len(ec2_instances)
    ll = len(df) - 1
    part_size = int(len(df) / n_part)

    i = 0
    jj = 0
    partitions = []
    processes = []
    while jj < ll:
        ii = i * part_size
        jj = part_size * (i + 1)
        if jj >= ll:
            jj = ll
        part = get_config(part_id=i, ix_0=ii, ix_n=jj, n_part=n_part, ifl=input_file_location,
                          ofl=output_folder_location, skf=ssh_key_file, au=aws_username, ain=aws_instance_name,
                          ar=aws_region, ac=aws_client, script_name=script_name, scripts_location=script_location,
                          ds=data_source, pcid=power_column_id, tsi=time_shift_inspection)
        partitions.append(part)
        create_partition(part, i, ec2_instances[i])
        i += 1
