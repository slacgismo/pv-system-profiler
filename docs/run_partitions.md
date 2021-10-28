<!-- ## This document walks through how to run the partition script that runs the parameter_estimation_script.py in parallel using EC2 instances within the GISMo team -->

#### Note: parameter_estimation_script: is the meat of the application and run_partition_script.py runs the parameter_estimation_script.py


# Pv-system-profiler Partitions Running Steps From an existing AMI image:

### Step 1 - On AWS EC2, start creating an instance using pv-system-profiler-prod AMI image.

##### &nbsp;&nbsp;&nbsp;&nbsp; Instance Tier: We have been selecting M4 as the instance tier.
##### &nbsp;&nbsp;&nbsp;&nbsp; Configure Instance:
##### &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Chose number of instances desired. 20 instances take about 5 hours to run with default configuration.
##### &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Subnet: subnet-2893a173...
##### &nbsp;&nbsp;&nbsp;&nbsp; Add Tags: (we add 2 tags)
##### &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Key = "Name", Value = (add desired value for the name of the ec2 instance)
##### &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Key = "Project", Value = "pv-insight" (unless used for another project)
##### &nbsp;&nbsp;&nbsp;&nbsp; Configure Security Group: choose "pvinsight-scsf"
##### &nbsp;&nbsp;&nbsp;&nbsp; Select Review and Launch and Launch after reviewing with desired key pair for accessing the server

#### NOTE: This instance already comes packed with mosek, pvi-user conda env, pvsystemprofiler source code

### Step 2 - Start up all the EC2 instances created
##### &nbsp;&nbsp;&nbsp;&nbsp; Login to one of them: it should come with everything needed including a bash file. Change the partition_name at the the of the python kwargs to match with the current EC2 instance, making sure all the desired ec2 instances are named the same.

##### &nbsp;&nbsp;&nbsp;&nbsp; Run git pull: ```git pull origin master```
##### &nbsp;&nbsp;&nbsp;&nbsp; Run Site Reports on partitions: ```bash parameter_estimation.sh```
##### &nbsp;&nbsp;&nbsp;&nbsp; NOTE: pvsystemprofiler/scripts/modules/create_partition.py lines 53-54 comment to allow resume functionality, when done in each seperate run (so if you'd like to start a different run from scratch) you would have to manually remove the folders created for the run.


For gismo collaborators: check out the video in https://drive.google.com/drive/folders/1cNTbzUTWVoQfvO5as_vQ9xW82Us0GVuA for more info.

# Pv-system-profiler Partitions Running Steps From Scratch:

#### Create an Ubuntu 20.04 ec2 instance

```
sudo apt update
sudo apt upgrade
# install miniconda
# https://dev.to/waylonwalker/installing-miniconda-on-linux-from-the-command-line-4ad7
# install gcc
sudo apt install g++
# download  yaml file to build conda env
curl -O https://raw.githubusercontent.com/slacgismo/pv-system-profiler/master/pvi-user.yml
# setup conda environment
conda env create -f pvi-user.yml
```
Request an academic Mosek license: https://www.mosek.com/products/academic-licenses/
In your local machine
```
	# copy file into ec2 from your local machine while ubuntu running on ssh
scp -i path/to/pem/file path/to/local/mosek.lic ubuntu@my-instance-public-dns-name:~/mosek
```
```
	# activate conda env
conda activate pvi-user
```
