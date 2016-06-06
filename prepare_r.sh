##	run on master
##	manual copy data to master
##  Use this to copy a file from s3 to the cluster
#   sudo aws s3 cp s3://nerdery2016/prepare_r.sh  /home/hadoop/
## 	Make file executable
#	sudo chmod +x prepare_r.sh
## run file
#   ./prepare_r.sh

#Add rstudio user to hadoop group
sudo usermod -a -G hadoop rstudio

#Install R Studio Server (see R Studio site to verity current version)
sudo wget https://download2.rstudio.org/rstudio-server-rhel-0.99.902-x86_64.rpm
sudo yum install --nogpgcheck rstudio-server-rhel-0.99.902-x86_64.rpm
#  sudo rstudio-server verify-installation

#create data directory
sudo mkdir data_local

#load and unzip
sudo aws s3 cp s3://nerdery2016/loan.csv.zip  /home/hadoop/data_local
sudo unzip /home/hadoop/data_local/loan.csv.zip -d /home/hadoop/data_local/

sudo aws s3 cp s3://nerdery2016/loan.zip  /home/hadoop/data_local
sudo unzip /home/hadoop/data_local/loan.zip -d /home/hadoop/data_local/

#copy existing R file, if returning to project
sudo aws s3 cp s3://nerdery2016/bigdata2016.R /home/rstudio/

#change the ownership:group of the rstudio file
sudo chown rstudio:rstudio /home/rstudio/bigdata2016.R 

#Create Hadoop Directory
hadoop fs -mkdir /data
hadoop fs -mkdir /data/raw
hadoop fs -mkdir /data/clean

#Move file to hadoop
hadoop fs -put /home/hadoop/data_local/loan.csv /data/raw/
hadoop fs -put /home/hadoop/data_local/loan.txt /data/clean/

#Copy Hive query script
sudo aws s3 cp s3://nerdery2016/loan.hql  /home/hadoop/loan.hql

#Create Hive Tables, write stdout and stderr to log.  
nohup hive -f /home/hadoop/loan.hql > .errorloan.log 2>&1

#Copy R file back to S3 when done for the day
## File located in /home/rstudio/
# sudo aws s3 cp /home/rstudio/bigdata2016.R s3://nerdery2016/bigdata2016.R

## Set Hue Username and Password
#username: hdfs
#password: Hadoop123*