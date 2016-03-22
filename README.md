## graph_lfs_stats

Script is designed to poll md_stat for either an MDT or an OSS. merges that data and some datapoints from facter in order to update a graphite system. 
In our implementation we are firing this out in crontab on a interval but you can also use any other scheduling tool. 

```
usage: graph_lfs_stats.py [-h] [-m] [-o] [-f FILE_LOCATION] [-d DATACENTER]
                          [-n HOSTNAME] [-i INTERVAL] [-v]

polling lustre for statistics to pump into graphite host

optional arguments:
  -h, --help            show this help message and exit
  -m, --mds             parsing md_stat on and MDS host
  -o, --oss             parsing stats on and OSS host
  -f FILE_LOCATION, --file-location FILE_LOCATION
                        location of mds or oss datafile, default is mds
                        /proc/fs/lustre/mdt/bulfs01-MDT0000/md_stats, when oss
                        is enabled odbfilter will be envoked.
  -d DATACENTER, --datacenter DATACENTER
                        Pass datacenter value for graphite ingest string to
                        parse. eg. (holyoke, 1ss, 60ox)
  -n HOSTNAME, --hostname HOSTNAME
                        Pass shortname hostname value for graphite ingest
                        string to parse. eg. (rcwebsite2)
  -i INTERVAL, --interval INTERVAL
                        manipulate sample interval of data polling. Value in
                        seconds
  -v, --verbose         verbose output
```
###Config parameters can be set in the main section of the code at the very bottom of the graph script: 
````
  #Global Parameters can be set here:
  ## Log related setting
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  log_location ='<Custom log path>'
  logger = logging.getLogger(log_location)
  ## Use these if you use facter in your environment
  facter_json_file_location = '<Facter file facts.json>'
  facter_json_location = '<Facter file parent path>'
  ## graphite server settings 
  graphite_server = '<graphite server hostname>'
  graphite_port = <Graphite listner port>
  graphite_service_name = '<Graphite service name>'
  ````