import sys
import pandas as pd
import config
from run_partition import run_config
from run_partition import get_address

if __name__ == '__main__':
    input_file_location = str(sys.argv[1])
    ssh_key_file = str(sys.argv[2])
    aws_username = str(sys.argv[3])
    aws_instance_name = str(sys.argv[4])
    aws_region = str(sys.argv[5])
    aws_client = str(sys.argv[6])

    main_class = config.get_config(ifl=input_file_location, skf=ssh_key_file, au=aws_username, ain=aws_instance_name,
                                   ar=aws_region, ac=aws_client)

    ec2_instances = get_address(aws_username, ssh_key_file, aws_instance_name, aws_region, aws_client)

    df = pd.read_csv(input_file_location, index_col=0)
    n_part = len(ec2_instances)
    #
# ll = len(df) - 1
# chunk_size = int(len(df) / n_part)
#
# i = 0
# jj = 0
#
# partitions = []
# processes = []
#
#     while jj < ll:
#         ii = i * chunk_size
#         jj = chunk_size * (i + 1)
#         if jj >= ll:
#             jj = ll
#         part = config.get_config(chunk_id= i, ix_0=ii, ix_n=jj, n_chunks=n_part)
#         partitions.append(part)
#         run_config(part, i, ec2_instances[i])
#         i += 1
