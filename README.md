# pv-system-profiler
Estimating PV array location and orientation from real-world power datasets.
## Run Scripts
### Serial run
The site_report script creates a report of all systems based on the csv files with the sytem signals located in a given folder.
The script takes all input parameters as kwargs. The example below illustrates the use of report_script:
```sh
python /"repository location"/pv-system-profiler/pvsystemprofiler/scripts/site_report.py
None all s3://"s3 bucket location with csv signals"/ None power_inv /"output folder location"/results.csv
True False False False None
```
where the individual value of each kwarg are defined in site_report.py. In this example, the folder with the csv input
signals is located in an Amazon Web Services (AWS) bucket. It can also be located in a local folder. Results are written to
the file results.csv in a local directory
Similar kwargs are entered for the longitude_script, latitude_script and the tilt_azimuth script. Here is an example:
```shell
python /"repository location"/pv-system-profiler/pvsystemprofiler/scripts/tilt_azimuth_script.py
None all s3://"s3 bucket location with csv signals"/ None power_inv_  /"output folder location"/results.csv
 True False False False s3://"s3 bucket location with longitude and latitude precalculates"/lon_lat_precalculates.csv
```
here, the last kwarg provides the location of a site list with previously calculated values of longitude and latitude
and the true values. In the case of the longitude and latitude scripts, a list with the true values of longitude and
latitude can be provided. It is also possible to provide no input list at all.
 ## Partitioned run
A script that runs the site report and the longitude, latitude and tilt and azimuth scripts using a number of prescribed AWS
instances is provided. The script reads the folder containing the system signals and partitions these signals to run in
a `n` user prescribed AWS instances in parallel. Here is an example shell command for a partitioned run:
```shell
python /"repository location"/pv-system-profiler/pvsystemprofiler/scripts/run_partition_script.py None all 
/"repository location"/pv-system-profiler/pvsystemprofiler/scripts/tilt_azimuth_script.py "conda_environment" 
None power_inv_ False False False False 
s3://"s3 bucket location with longitude and latitude precalculates"/lon_lat_precalculates.csv "instance_name"
 "s3 bucket with signals" None aws
```
where the individual value of each kwarg are defined in run_partition_script.py. Previous to running this command it is
necessary to create `n` identical AWS instances that correspond to the number of desired partitions. These instances
need to have the same `Name="instance name"` AWS tag. The simplest way to accomplish this is by parting from an AWS image of a
previously configured instance. This image needs to have all the  repositories and conda environments that would be
needed in a serial run. Once each partitioned run is finished, results will be automatically collected in the local folder where
`run_script.py` was run. The kwarg `"instance_name"` corresponds to the conda environment that is to be used in the
partitioned run.
