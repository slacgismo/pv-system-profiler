<!-- ## This document walks through how to run the partition script that runs the parameter_estimation_script.py in parallel using EC2 instances within the GISMo team -->

#### Note: parameter_estimation_script: is the meat of the application and run_partition_script.py runs the parameter_estimation_script.py


# Pv-system-profiler Partitions Running Steps From an existing AMI image:

## Step 1 - On AWS EC2, start creating an instance using pv-system-profiler-prod AMI image. (SKIP STEP 1 if instances already exist and are up and running)

Instance Tier: We have been selecting M4 as the instance tier.

Configure Instance:
* Chose number of instances desired. 20 instances take about 5 hours to run with default configuration.
* Subnet: subnet-2893a173...

Add Tags: (we add 2 tags)
* Key = "Name", Value = (add desired value for the name of the ec2 instance)
* Key = "Project", Value = "pv-insight" (unless used for another project)

Configure Security Group: choose "pvinsight-scsf"

Select Review and Launch and Launch after reviewing with desired key pair for accessing the server

#### NOTE: This instance already comes packed with mosek, pvi-user conda env, pvsystemprofiler source code


## Step 2 - Start up all the EC2 instances created
Login to one of them: it should come with everything needed including a bash file. Change the **EC2_NAME** in the **partition_script_template.sh** (configuration for running estimations/site_report) match with the current EC2 instance, making sure all the desired ec2 instances are named the same.

1) Run git pull in the repo located in the pv-system-profiler repo located in /home/ubuntu/github/pv-system-profiler:
```git pull origin master```

2) Checkout **partition_script_template.sh** in ```/home/ubuntu``` to configure and customize the run. Creating a copy of the template is advised if changes are to be made.

3) Navigate to ```/home/ubuntu``` and run desired estimation/site_report on partitions:
```bash partition_script_template.sh```

NOTE:
* pvsystemprofiler/scripts/modules/create_partition.py lines 53-54 comment to allow resume functionality, when done in each separate run (so if you'd like to start a different run from scratch) you would have to manually remove the folders created for the run.

* it is advised to remove all the folders+files after each run that are not needed. (everything except /home/ubuntu/github, /home/ubuntu/miniconda3, /home/ubuntu/mosek, /home/ubuntu/partition_script_template.sh)

For gismo collaborators: check out the video in https://drive.google.com/drive/folders/1cNTbzUTWVoQfvO5as_vQ9xW82Us0GVuA for more info.

# Pv-system-profiler Partitions Running Steps w/o the existing AMI Images:

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
