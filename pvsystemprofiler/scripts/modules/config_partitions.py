class ConfigPartitions:
    def __init__(self, part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, ip_address=None, skf=None,
                 au=None, ain=None, ar=None, ac=None, script_name=None, scripts_location=None, pcid=None, gof=None,
                 god=None, tsi=None, s3l=None, n_files=None, file_label=None, fix_time_shifts=None,
                 time_zone_correction=None, check_json=None, sup_file=None):

        self.input_file_location = ifl
        self.ssh_key_file = skf
        self.aws_username = au
        self.aws_instance_name = ain
        self.aws_region = ar
        self.aws_client = ac
        self.supplementary_file = sup_file
        if ix_0 is not None and ix_n is not None:

            self.part_id = part_id
            self.env_name = 'partition_' + str(part_id)
            self.n_part = n_part
            self.local_working_folder_location = ofl
            self.local_working_folder = self.local_working_folder_location + 'estimation_part_{}_of_{}/'.format(
                str(part_id + 1), str(n_part))
            self.local_input_file = self.local_working_folder + 'data/input_part_{}_of_{}.csv'.format(str(part_id + 1),
                                                                                                      str(n_part))
            self.local_output_file_name = 'results_part_{}_of_{}.csv'.format(str(part_id + 1), str(n_part))
            self.local_output_file = self.local_working_folder + 'data/' + self.local_output_file_name
            self.ix_0 = ix_0
            self.ix_n = ix_n
            self.scripts_location = scripts_location
            self.script_name = self.scripts_location + script_name
            self.power_column_id = pcid
            self.time_shift_inspection = tsi
            self.public_ip_address = ip_address
            self.process_completed = False
            self.s3_location = s3l
            self.n_files = n_files
            self.file_label = file_label
            self.fix_time_shifts = fix_time_shifts
            self.time_zone_correction = time_zone_correction
            self.check_json = check_json



        else:
            self.global_output_directory = god
            self.global_output_file = god + gof


def get_config(part_id=None, ix_0=None, ix_n=None, n_part=None, ifl=None, ofl=None, ip_address=None, skf=None, au=None,
               ain=None, ar=None, ac=None, script_name=None, scripts_location=None, pcid=None, gof=None, god=None,
               tsi=None, s3l=None, n_files=None, file_label=None, fix_time_shifts=None, time_zone_correction=None,
               check_json=None, sup_file=None):
    if ix_0 is not None and ix_n is not None:
        return ConfigPartitions(part_id=part_id, ix_0=ix_0, ix_n=ix_n, n_part=n_part, ifl=ifl, ofl=ofl,
                                ip_address=ip_address, skf=skf, au=au, ain=ain, ar=ar, ac=ac, script_name=script_name,
                                scripts_location=scripts_location, pcid=pcid, tsi=tsi, s3l=s3l, n_files=n_files,
                                file_label=file_label, fix_time_shifts=fix_time_shifts,
                                time_zone_correction=time_zone_correction, check_json=check_json, sup_file=sup_file)
    else:
        return ConfigPartitions(ifl=ifl, skf=skf, au=au, ain=ain, ar=ar, ac=ac, gof=gof, god=god)
