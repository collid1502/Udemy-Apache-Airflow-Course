<<c
This script captures all details of setting up Apache Airflow based on the instructions from the 
Udemy Course: Apache Airflow - Complete Hands On Beginner to Advanced Class

Capturing all the Linux commands undertaken to set up the environment for this learning project 

useful site:  https://www.astronomer.io/guides/airflow-wsl/ 
c

# update & upgrade to ensure everything is up to date  (will prompt for Ubuntu user password)  
sudo apt update && sudo apt upgrade

# clear all text after upgrades 
clear 


<<c
by default, the C drive is available through the /mnt/ folder. This can cause issues later with Airflow, 
so this will be updated now to reflect this.

We need to open up the WSL conf file, and make some adjustments, using `nano` the CLI editor on Linux
c

# execute below line and it will open the nano editor. (either pre-existing file, or blank new one if no file existed) 
sudo nano /etc/wsl.conf 

<<c
Once file editor is open, add in:

    [automount]
    root = / 
    options = "metadata"

Then, ctrl+s to save the file, ctrl+x to exit the file.
For this new config to take effect, you must SIGN OUT of windows, then sign back in.
c

# after signing back in, relaunch Ubuntu 
# to check that the config change worked, and that the C drive is now available at teh same level as /mnt/ rather than within it
# do the following:
cd ..
ls
cd ..
ls
 # you should now see a list of all the directories at the top of your distro. `mnt` will be in there, and now too should be the `c` folder

# head back to `home`
cd home/collid/

 # ------------------------------------------------------------------------------------------------------------------------------------------------

# validate that python 3 is installed (at time of writing, v 3.8.10 was installed)
python3 --version 

# now, install pip 
sudo apt update 
sudp apt install python3-pip 

# validate pip installed correctly
pip3 --version 

# now we can install apache airflow (along with a few extra packages; gcp, statsd & sentry)
pip3 install apache-airflow[gcp,statsd,sentry]==1.10.10 

# now install cryptography - can be used to encrypt passwords for connections, such as to Google Cloud Platform
pip install cryptography==2.9.2

# then install pyspark v2.4.5
pip install pyspark==2.4.5 


# set airflow home environment variable - we have created an emoty folder called `airflow` in the location specified below 
export AIRFLOW_HOME=/c/users/dan/airflow

# to avoid needing to export this variable each time we launch ubuntu, we can add it to the .bashrc file
# this will make it available for us each time we launch ubuntu 
# use nano text editor to modify the bashrc file 
nano ~/.bashrc 

<<c
Once the file editor opens, anywhere near the top, just simply add a new line with the following:

    export AIRFLOW_HOME=/c/users/dan/airflow

once done, use ctrl+s to save, ctrl+x to exit
c

# close ubuntu, then relaunch it 
# validate airflow installation by checking version  (note, had error asking to install a missing package so i PIP installed it first, then checked version again)

# should print 1.10.10 to the terminal 
airflow version 

#validate the airflow home env variable is also correct 
echo $AIRFLOW_HOME 

# should see `/c/users/dan/airflow` printed to terminal 

# ----------------------------------------------------------------------------------------------------------

# Now, let's initialise the Airflow Metadata Database (creates all needed tables for Airflow in order to run) 
airflow initdb 

# can then run airflow webserver   can use -p 8080 or 8081 etc to determine port that server runs on local host 
airflow webserver -p 8080   

# now, you can open a browser and go to `localhost:8080` you will see the Web UI 

# you will see a message informing the scheduler is not yet running. 
# we cannot run the scheduler in the same window, so, open another Ubuntu terminal and do:
airflow scheduler

<<c
when attempting this, just like the instructor, there was a permissions error for this step.
As such, the solution was to modify the airflow config (using windows file explorer) with these steps:

> navigate to C:\Users\Dan\airflow 
> open the airflow cfg file to edit it 
> search for `load_examples`
> Change it from True to False 
> save the change, exit the file
> head back to ubuntu, kill the webserver, then re-initialise the airflow database 
c

# initialise db 
airflow initdb 

# initialise webserver & navigate to localhost:8080 on your browser 
airflow webserver 


# open the second ubuntu terminal, and start the airflow scheduler 
airflow scheduler 

# if you go back to your browser & refresh, you will see the warning about the scheduler has now gone 

<<c
So, from now on anytime you wish to start Airflow and use the UI, you can come to Ubuntu, and open terminals 
to start the Webserver & Scheduler.

Now, any DAGs we create, will need to be stored within the `airflow` folder created earlier and referenced above in the 
$AIRFLOW_HOME env variable.

With this in minf=d, it's worth creating a new folder there, called `dags`
You will put all yopur future DAGs, Python configuration files etc here later on 
c

# end 