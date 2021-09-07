# pv-system-profiler

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


Estimating PV array location and orientation from real-world power datasets.
## Usage / Run Scripts
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
python /"repository location"/pv-system-profiler/pvsystemprofiler/scripts/run_partition_script.py
None all /"repository location"/pv-system-profiler/pvsystemprofiler/scripts/tilt_azimuth_script.py conda_environment
None power_inv_ True False True False  "instance_name"
s3://"s3 bucket location with longitude and latitude precalculates"/lon_lat_precalculates.csv
```
where the individual value of each kwarg are defined in run_partition_script.py. Previous to running this command it is
necessary to create `n` identical AWS instances that correspond to the number of desired partitions. These instances
need to have the same `Name="instance name"` AWS tag. The simplest way to accomplish this is by parting from an AWS image of a
previously configured instance. This image needs to have all the  repositories and conda environments that would be
needed in a serial run. Once each partitioned run is finished, results will be automatically collected in the local folder where
`run_script.py` was run. The kwarg `"instance_name"` corresponds to the conda environment that is to be used in the
partitioned run.

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
