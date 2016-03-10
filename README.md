## graph_lfs_stats

Script is designed to poll md_stat for either an MDT or an OSS. merges that data and some datapoints from facter in order to update a graphite system. 
In our implementation we are firing this out in crontab on a interval but you can also use any other scheduling tool. 
