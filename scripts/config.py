class ConfigPartitions:
    def __init__(self, part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, skf=None,
                 au=None, ain=None, ar=None, ac=None, script_location=None):
        self.input_file_location = ifl
        self.ssh_key_file = skf
        self.aws_username = au
        self.aws_instance_name = ain
        self.aws_region = ar
        self.aws_client = ac
        if ix_0 is not None and ix_n is not None:
            self.output_folder_location = ofl
            self.part_id = part_id
            self.env_name = 'partition_' + str(part_id)
            self.n_part = n_part
            self.output_folder = 'results_longitude_{}_of_{}/'.format(str(part_id), str(n_part))
            self.output_file = self.output_folder + 'data/results_longitude_{}_of_{}.csv'. \
                format(str(part_id), str(n_part))
            self.ix_0 = ix_0
            self.ix_n = ix_n
            self.script_location = script_location


def get_config(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, skf=None, au=None, ain=None,
               ar=None, ac=None, script_location=None):
    if ix_0 is not None and ix_n is not None:
        return ConfigPartitions(part_id=part_id, ix_0=ix_0, ix_n=ix_n, n_part=n_part, ifl=ifl,  ofl=ofl, skf=skf,
                                au=au, ain=ain, ar=ar, ac=ac, script_location=script_location)
    else:
        return ConfigPartitions(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=ifl,  ofl=None, skf=skf, au=au,
                                ain=ain, ar=ar, ac=ac)
