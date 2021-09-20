# pv-system-profiler
### Estimating PV array location and orientation from real-world power datasets.

<table>
<tr>
  <td>Latest Release</td>
  <td>
    <a href="https://pypi.org/project/pv-system-profiler/">
        <img src="https://img.shields.io/pypi/v/pv-system-profiler.svg" alt="latest release" />
    </a>
    <a href="https://anaconda.org/slacgismo/pv-system-profiler">
        <img src="https://anaconda.org/slacgismo/pv-system-profiler/badges/version.svg" />
    </a>
    <a href="https://anaconda.org/slacgismo/pv-system-profiler">
        <img src="https://anaconda.org/slacgismo/pv-system-profiler/badges/latest_release_date.svg" />
    </a>
</tr>
<tr>
  <td>License</td>
  <td>
    <a href="https://github.com/slacgismo/pv-system-profiler/blob/master/LICENSE">
        <img src="https://img.shields.io/pypi/l/pv-system-profiler.svg" alt="license" />
    </a>
</td>
</tr>
<tr>
  <td>Build Status</td>
  <td>
    <a href="https://app.circleci.com/pipelines/github/slacgismo/pv-system-profiler">
        <img src="https://circleci.com/gh/slacgismo/pv-system-profiler.svg?style=svg" alt="CircleCi build status" />
    </a>
  </td>
</tr>
<tr>
    <td>Code Quality</td>
    <td>
        <a href="https://lgtm.com/projects/g/slacgismo/pv-system-profiler/context:python">
            <img alt="Language grade: Python" src="https://img.shields.io/lgtm/grade/python/g/slacgismo/pv-system-profiler.svg?logo=lgtm&logoWidth=18"/>
        </a>
        <a href="https://lgtm.com/projects/g/slacgismo/pv-system-profiler/alerts/">
            <img alt="Total alerts" src="https://img.shields.io/lgtm/alerts/g/slacgismo/pv-system-profiler.svg?logo=lgtm&logoWidth=18"/>
        </a>
    </td>
</tr>
<tr>
    <td>Publications</td>
    <td>
      <a href="https://zenodo.org/badge/latestdoi/183074637">
        <img src="https://zenodo.org/badge/183074637.svg" alt="DOI">
      </a>
    </td>
</tr>
<tr>
    <td>PyPI Downloads</td>
    <td>
        <a href="https://pepy.tech/project/pv-system-profiler">
            <img src="https://img.shields.io/pypi/dm/pv-system-profiler" alt="PyPI downloads" />
        </a>
    </td>
</tr>
<tr>
    <td>Conda Downloads</td>
    <td>
        <a href="https://anaconda.org/slacgismo/pv-system-profiler">
            <img src="https://anaconda.org/slacgismo/pv-system-profiler/badges/downloads.svg" alt="conda-forge downloads" />
        </a>
    </td>
</tr>
</table>

## Install & Setup

#### 1) Recommended: Set up `conda` environment with provided `.yml` file

We recommend setting up a fresh Python virtual environment in which to use `pv-system-profiler`. We recommend using the [Conda](https://docs.conda.io/projects/conda/en/latest/index.html) package management system, and creating an environment with the environment configuration file named `pvi-user.yml`, provided in the top level of this repository. This will install the `statistical-clear-sky` and `solar-data-tools` packages as well.

Creating the env:

```bash
$ conda env create -f pvi-user.yml
```

Starting the env:

```bash
$ conda activate pvi_user
```

Stopping the env

```bash
$ conda deactivate
```

Additional documentation on setting up the Conda environment is available [here](https://github.com/slacgismo/pvinsight-onboarding/blob/main/README.md).


#### 2) PIP Package

```sh
$ pip install pv-system-profiler
```

Alternative: Clone repo from GitHub

Mimic the pip package by setting up locally.

```bash
$ pip install -e path/to/root/folder
```

#### 3) Anaconda Package

```sh
$ conda install -c slacgismo pv-system-profiler
```

## Solver Dependencies

Refer to [solar-data-tools](https://github.com/slacgismo/solar-data-tools) documentation to get more info about solvers being used.

## Usage / Run Scripts
### Serial run
The `parameter_estimation_script.py` script creates a report of all systems based on the `csv` files with the system signals located in a given folder.
The script takes all input parameters as `kwargs`. The example below illustrates the use of report_script:
```shell
python 'repository location of run script'/parameter_estimation_script.py report None all 
s3://s3_bucket_with_signals/ 'repeating_part_of label' /home/results.csv True False 
False False s3://'s3_path_to_file_containing_metadata/metadata.csv' None s3
```
In the example above the full path to `parameter_estimation_script.py` is specified to run a
`report`. The script allows to provide a `csv` file with list of sites to be analyzed. In this case no list is provided 
and therefore the `kwarg` `None` is entered. The script also allows to run an analysis on the first `n_files` containing 
input signals in the `s3` repository. In this, case the `all` `kwarg` specifies that all input signals are to be analyzed. 
In this example, all `csv` files containing the input signals are located in the `s3` bucket with the name 
`s3://s3_bucket_with_signals/`. Usually these `csv` files are of the form `ID_repeating_part_of_label.csv`, for example:
`1_composite_10.csv`, `2_composite_10.csv`, where `_composite_10` is the repeating part of the label. The repeating part 
of the label is either None or a string as in the example above. Next, an absolute path to the desired location of the 
results file is provided, in this case `/home/results.csv`. The two following `kwargs` are type Boolean and are used to set the 
values of the `correct_tz` and `fix_shifts` pipeline `kwargs`. The next  `kwarg`,  `check_json` is also Boolean. It 
is used to indicate if  there is a `json` file present in `s3://s3_bucket_with_signals/` with additional site information 
that is  to be analyzed. The next Boolean `kwarg` is used to set the `convert_to_ts` `kwarg` when instantiating the data 
handler.  The next `kawrg` contains the full path to the `csv` file containing site metadata, here called `metadata.csv`. 
The information that this file should contain varies depending on the `estimation` to be performed. This file is 
optional and the `kwarg` can be set to `None`. For the case of a `report`, a `csv` file with columns labeled `site`, 
`system` and `gmt_offset` and their respective values need to be provided. Alternatively, if the `gmt_offset` `kwarg`, 
the next `kwarg` (in the example above set to `None`), has a numeric 
value different to `None`, all sites will use that single value when running the report. For the case of the `report` 
estimation, the metadata file should contain `site`, `system` and `gmt_offset` columns with the respective
values for each system. For the case of the `longitude` estimation, the metadata file should contain `site`, `system` 
and `latitude` columns with the respective values for each system. For the case of the `tilt_azimuth` estimation, the 
metadata file should contain `site`, `system`, `gmt_offset`, `estimated_longitude` and `estimated_latitude`, `tilt`, 
`azimuth` columns and with the respective values for each system. Additionally, if a manual inspection for time shifts 
was performed, another 
column labeled `time_shift_manual` having a zero for systems with no time shift and ones for systems with time shift
may be included. If a `time_shift_manual` column is included, it will be used to determine whether the `fix_dst()` 
method is run after instantiating the data handler. The next `karg is` `gmt_offset` and in this case it is set to None. 
The last kwarg corresponds to the `data_source`. In this case the value is `s3` since files with the input signals are
located in an `s3` bucket.
 ## Partitioned run
A script that runs the site report, the longitude, latitude and tilt and azimuth scripts using a number of prescribed 
Amazon Web Services (AWS), instances is provided. The script reads the folder containing the system signals and 
partitions these signals to run in  a `n` user prescribed AWS instances in parallel. Here is an example shell command 
for a partitioned run:
```shell
python 'repository location of run script'/run_partition_script.py parameter_estimation_script.py report None all 
s3://s3_bucket_with_signals/ 'repeating_part_of label' /home/results.csv True False 
False False s3://'s3_path_to_file_containing_metadata/metadata.csv' None s3
'repository location of run script'/parameter_estimation_script.py pvi-dev my_instance
 ```
where the individual value of each kwarg are defined in run_partition_script.py. This script takes the same inputs as
the `parameter_estimation_script.py` plus three additional parameters. Note that the first kwarg is the partitioning 
script repository location of run script `/run_partition_script.py parameter_estimation_script.py`. The estimation run
script `/parameter_estimation_script.p` is specified as the third to last kwarg. The second to last kwarg is the conda
enviroment to be used to run the estimation, in this case `pvi-dev`. The last kwarg is the name of the AWS instances to
be used to run `run_partition_script.py`, in this case `my_instance`. Previous to running this command it is necessary to create `n` identical AWS 
instances that correspond to the number of desired partitions. These instances need to have the same 
`Name='instance name'` AWS tag. The simplest way to accomplish this is by parting from an AWS  image of a  previously 
configured instance. This image needs to have all the  repositories and conda environments that 
would be  needed in a serial run. Once each partitioned run is finished, results will be automatically collected in the 
local folder where `run_partition_script.py` was run. 
## Unit tests

In order to run unit tests:
```
python -m unittest -v
```

## Test Coverage

In order to view the current test coverage metrics:
```
coverage run --source pvsystemprofiler -m unittest discover && coverage html
open htmlcov/index.html
```

## Versioning

We use [Semantic Versioning](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/slacgismo/pv-system-profiler/tags).

## License

This project is licensed under the BSD 2-Clause License - see the [LICENSE](LICENSE) file for details
