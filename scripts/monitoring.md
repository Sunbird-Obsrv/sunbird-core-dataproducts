## Instances ##

### Dev ###

1. Secor - 54.179.138.122 - (secor-raw & secor-me)
2. Spark - 54.169.228.223 - (cassandra-service & api-service)

### QA ###

1. Secor - 54.169.51.61 - (secor-raw & secor-me)
2. Spark - 54.254.152.135 - (cassandra-service & api-service)

### Prod ###

1. Secor - 52.77.212.151 - (secor-raw & secor-me)
2. Spark - 54.169.146.32 - (api-service)
3. Cassandra - 54.169.179.102 - (cassandra-service)

***

## Services ##

All the processes are servicified and follow a common calling mechanism

```sh 
# Check if the service is running
<service> status

# Returns
# Checking <service> ...                            [ RUNNING ]
# Checking <service> ...                            [ STOPPED ]
# Checking <service> ...   Process dead but pidfile exists

# Stop service
<service> stop

# Returns
# Shutting down  <service> ...                       [ OK ]
# Shutting down  <service> ...                       [ FAIL ]
# Shutting down  <service> ...                       [ NOT RUNNING ]

# Start service
<service> start

# Returns
# Starting <service> ...                            [ OK ]
# Starting <service> ...                            [ FAIL ]
# Process dead but pidfile exists
# <service> is already running!

# Restart service
<service> start

# Returns
# Shutting down  <service> ...                      [ OK | FAIL | NOT RUNNING ]
# Starting <service> ...                            [ OK | FAIL ]
```

### Secor ###

**1) Secor raw telemetry sync**

```sh 
# Check if the process is running
/home/ec2-user/sbin/secor-raw status

# Stop Process
/home/ec2-user/sbin/secor-raw stop

# Start Process
/home/ec2-user/sbin/secor-raw start

# ReStart Process
/home/ec2-user/sbin/secor-raw restart
```

**2) Secor derived telemetry sync process**

```sh 
# Check if the process is running
/home/ec2-user/sbin/secor-me status

# Stop Process
/home/ec2-user/sbin/secor-me stop

# Start Process
/home/ec2-user/sbin/secor-me start

# ReStart Process
/home/ec2-user/sbin/secor-me restart
```

**3) Cassandra Process - Dev/QA**

```sh 
# Check if the process is running
/home/ec2-user/sbin/cassandra-service status

# Stop Process
/home/ec2-user/sbin/cassandra-service stop

# Start Process
/home/ec2-user/sbin/cassandra-service start

# ReStart Process
/home/ec2-user/sbin/cassandra-service restart
```

**4) Analytics API**

```sh 
# Check if the process is running
/home/ec2-user/sbin/api-service status

# Stop Process
/home/ec2-user/sbin/api-service stop

# Start Process
/home/ec2-user/sbin/api-service start

# ReStart Process
/home/ec2-user/sbin/api-service restart
```

**5) Cassandra Process - Prod**

Process running on - 54.169.179.102

```sh 
# Check if the cassandra is running
sudo service cassandra status # Would return "* Cassandra is running"

# Command to kill
sudo service cassandra stop

# Command to start the process
sudo service cassandra start
```
