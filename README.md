# Data_Engineering_1_Group_7_Project_VT25
Data Engineering course at Uppsala University project for group 7 during the Spring 2025 semester  

# Project Description
Reddit is an online platform where millions of people are able to interact online. Reddit contains subreddits which are smaller communities that are more specialized or niche. Within these Subreddits anyone can post, but generally the person who is the most active in a subreddit is considered a troll or shitposter. The goal of this project is to determine the 5 largest trolls on reddit.

# Dataset
The dataset we used contains 3.8 million reddit posts, is approximately 18.5 GB, and is located here https://zenodo.org/records/1043504#.Wzt7PbhXryo

# Distributed File System Architecture
We used Hadoop and Apache Spark for our distributed file system architecture. This is because Hadoop is fault tolerant and scalable and is able to work with large datasets such as the one that we have. Spark is easy to use with PySpark and integrates well with Hadoop. Spark is also horizontally scalable which is beneficial to have if the data ever increases in size.

# DFS setup
This setup will run through the set up that we did to get the Hapood cluster and Spark cluster up and running. The following code was added to every VM's /etc/hosts file so the VM's could communicate with each other.  
```
192.168.2.89 group-7-headnode
192.168.2.236 group-7-wrokernode
192.168.2.161 group-7-workernode2
192.168.2.149 group-7-workernode3
192.168.2.248 elis-amcoff
192.168.2.240 caleb-driver
192.168.2.187 albert-alpsten-A3
192.168.2.6 denicewikstrombm
192.168.2.114 luchen-devm-lab3
```
## Hadoop
#### Masternode
On a newly created VM with that had 8 cores and 40 GB memory we ran the following terminal commands  
```sudo apt update```  
```sudo apt-get install -y openjdk-8-jdk```  
```echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre" >> ~/.bashrc```  
```source ~/.bashrc```  
```cd ~```  
```wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz```  
```tar -xvf hadoop-3.3.6.tar.gz```  

We then set some configurations from  
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html  
Specifically we added to the ~/hadoop-3.3.6/etc/hadoop/core-site.xml file
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://192.168.2.89:9000</value>
    </property>
</configuration>
```
We then added to the ~/hadoop-3.3.6/etc/hadoop/hdfs-site.xml file  
```
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```
Then within the ~/hadoop-3.3.6 path we ran the following commands  
```/bin/hdfs namenode -format```  
```/bin/hdfs --daemon start namenode```  

#### Datanode
On three other VMs we ran the following   
```sudo apt update```  
```sudo apt-get install -y openjdk-8-jdk```  
```echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre" >> ~/.bashrc```  
```source ~/.bashrc```  
```cd ~```  
```wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz```  
```tar -xvf hadoop-3.3.6.tar.gz```  

We then set some configurations from  
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html  
Specifically we added to the ~/hadoop-3.3.6/etc/hadoop/core-site.xml file
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://192.168.2.89:9000</value>
    </property>
</configuration>
```
We then added to the ~/hadoop-3.3.6/etc/hadoop/hdfs-site.xml file  
```
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```
Then within the ~/hadoop-3.3.6 path we ran the following commands on only one of the three VMs
```/bin/hdfs --daemon start datanode```  
With Hadoop set up we ran the following code in the same directory as our json file to upload it to the hdfs.  
```hdfs dfs -put /corpus-webis-tldr-17.json /user/ubuntu/corpus/```  

## Spark
#### Namenode  
We used the Hadoop Masternode as our Spark cluster namenode as well so we ran the following commands  
```cd ~```  
```wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz```  
Note: we used spark 3.5.5 since spark 3.5.0 could not be found  
```tar -zxvf spark-3.5.5-bin-hadoop3.tgz```  
```echo "export SPARK_HOME=~/spark-3.5.5-bin-hadoop3" >> ~/.bashrc```  
```source ~/.bashrc```  
```cd ~/spark-3.5.0-bin-hadoop3/```  
Then to start the Namenode we ran  
```/sbin/start-master.sh```  
We also started a worker on this VM as well with  
```/sbin/start-worker.sh spark://192.168.2.89:7077```
#### Workernode 
On the same 3 VMs that we installed Hadoop on we ran  
```cd ~```  
```wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz```  
Note: we used spark 3.5.5 since spark 3.5.0 could not be found  
```tar -zxvf spark-3.5.5-bin-hadoop3.tgz```  
```echo "export SPARK_HOME=~/spark-3.5.5-bin-hadoop3" >> ~/.bashrc```  
```source ~/.bashrc```  
```cd ~/spark-3.5.0-bin-hadoop3/```  
Then to start the Workernodes we ran  
```/sbin/start-worker.sh spark://192.168.2.89:7077```

## Drivers
The goal is then use a driver VM similar to the one used in Lab 3 and connect to our own cluster to run our troll detection algorithm.  





