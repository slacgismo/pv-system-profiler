class ConfigPartitions:
    def __init__(self, part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, skf=None, au=None, ain=None, ar=None,
                 ac=None):
        if ix_0 is not None and ix_n is not None:
            self.chunk_id = partition_id
            self.env_name = 'partition_' + str(part_id)
            self.n_chunks = n_part
            self.results_file = './data/results_longitude_{}_of_{}.csv'.format(str(part_id), str(n_part))
            self.ix_0 = ix_0
            self.ix_n = ix_n
        else:
            self.input_file_location = ifl
            self.ssh_key_file = skf
            self.aws_username = au
            self.aws_instance_name = ain
            self.aws_region = ar
            self.aws_client = ac



def get_config(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, skf=None, au=None, ain=None, ar=None,
               ac=None):
    if ix_0 is not None and ix_n is not None:
        return ConfigPartitions(pid=part_id, ix_0=ix_0, ix_n=ix_n, n_part=n_part)
    else:
        return ConfigPartitions(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=ifl, skf=skf, au=au, ain=ain,
                                ar=ar, ac=ac)
