#/bin/bash

USER="rstudio"
USERPW="rstudio"


# create rstudio user on all machines
# we need a unix user with home directory and password and hadoop permission
sudo adduser $USER
sudo sh -c "echo '$USERPW' | passwd $USER --stdin"

# fix hadoop tmp permission on all machines
sudo chmod 777 -R /mnt/var/lib/hadoop/tmp