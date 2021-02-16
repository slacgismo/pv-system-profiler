import pandas as pd
import config
import config
from run_partition import run_config
from run_partition import get_address

main_class = config.get_config()
ssh_username = main_class.ssh_username
ssh_key_file = main_class.ssh_key_file
name = main_class.name
region = main_class.region
client = main_class.client
ec2_instances = get_address(ssh_username, ssh_key_file, name, region, client)

df = pd.read_csv(main_class.site_list_file, index_col=0)
n_part = len(ec2_instances)


ll = len(df) - 1
chunk_size =  int(len(df) / n_part)  

i = 0
jj = 0

partitions = []
processes = []
if __name__=='__main__':
    while jj < ll:
        ii = i * chunk_size
        jj = chunk_size * (i + 1)
        if jj >= ll:
            jj = ll
        part = config.get_config(chunk_id= i, ix_0=ii, ix_n=jj, n_chunks=n_part)
        partitions.append(part)
        run_config(part, i, ec2_instances[i])
        i += 1
