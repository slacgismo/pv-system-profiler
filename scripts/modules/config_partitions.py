class ConfigPartitions:
    def __init__(self, part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, skf=None,
                 au=None, ain=None, ar=None, ac=None, script_name=None, scripts_location=None, ds=None,
                 pcid=None, tsi=None):
        self.input_file_location = ifl
        self.ssh_key_file = skf
        self.aws_username = au
        self.aws_instance_name = ain
        self.aws_region = ar
        self.aws_client = ac
        if ix_0 is not None and ix_n is not None:

            self.part_id = part_id
            self.env_name = 'partition_' + str(part_id)
            self.n_part = n_part
            self.local_working_folder_location = ofl
            self.local_working_folder = self.local_working_folder_location + 'estimation_part_{}_of_{}/'.format(
                str(part_id + 1), str(n_part))
            self.local_input_file = self.local_working_folder + 'data/input_part_{}_of_{}.csv'.format(str(part_id + 1), str(n_part))
            self.local_output_file = self.local_working_folder + 'data/results_part_{}_of_{}.csv'.format(str(part_id + 1), str(n_part))
            self.ix_0 = ix_0
            self.ix_n = ix_n
            self.scripts_location = scripts_location
            self.script_name = self.scripts_location + script_name
            self.data_source = ds
            self.power_column_id = pcid
            self.time_shift_inspection = tsi


def get_config(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, skf=None, au=None, ain=None,
               ar=None, ac=None, script_name=None, scripts_location=None,  ds=None, pcid=None, tsi=None):
    if ix_0 is not None and ix_n is not None:
        return ConfigPartitions(part_id=part_id, ix_0=ix_0, ix_n=ix_n, n_part=n_part, ifl=ifl,  ofl=ofl, skf=skf, au=au,
                                ain=ain, ar=ar, ac=ac, script_name=script_name, scripts_location=scripts_location,
                                ds=ds, pcid=pcid, tsi=tsi)
    else:
        return ConfigPartitions(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=ifl,  ofl=None, skf=skf, au=au,
                                ain=ain, ar=ar, ac=ac)
