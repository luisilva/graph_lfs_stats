#!/usr/bin/python

import sys,os,json,argparse,logging,time,socket,threading
from subprocess import Popen, PIPE
from types import *
from threading import Thread

class lfs_stats:

  def __init__(self):
    self.argparser = self.argparser() # Get arguments from command line
    if not os.path.isfile(facter_json_file_location): #Check for json with host facts exsist if not generate one
      self.get_facts = self.get_facts()
    if self.datacenter == None or self.hostname == None:# If no facts have been passed on the command line lets get them from the json file
      self.dictify_facts = self.dictify_facts()
    self.get_epoch = self.get_epoch()# time stamp that we will use when we push to graphite
    if self.mds:# If flag for mds is set this means we are gathering MDS Stats.
      self.dictify_mdstat = self.dictify_mdstat()# 
      self.get_mds_delta = self.get_mds_delta()
      self.push_to_graphite = self.push_to_graphite()
    elif self.oss:# If flag for mds is set this means we are gathering OSS Stats.
      self.delta_oss_list = []
      self.dictify_oss = Thread(target = self.dictify_oss_stat()).start()
      self.get_oss_delta = self.get_oss_delta()
      self.dictify_brw = Thread(target = self.dictify_brw()).start()
      self.get_oss_brw_delta = self.get_oss_brw_delta()
      self.push_oss_to_graphite = self.push_oss_to_graphite()
     
  def argparser(self):
      #Setting up parsing options for inputting data
      parser = argparse.ArgumentParser(description="polling lustre for statistics to pump into graphite host")
      parser.add_argument("-m", "--mds", required=False,default=False,action='store_true', help="parsing md_stat on and MDS host")
      parser.add_argument("-o", "--oss", required=False,default=False,action='store_true', help="parsing stats on and OSS host")
      parser.add_argument("-f", "--file-location", required=False, default=None,help="location of mds or oss datafile, default is mds /proc/fs/lustre/mdt/<hostname>-MDT0000/md_stats,\n when oss is enabled odbfilter will be envoked.")
      parser.add_argument("-d", "--datacenter", required=False, default=None, help="Pass datacenter value for graphite ingest string to parse. eg. (holyoke, 1ss, 60ox)")
      parser.add_argument("-n", "--hostname", required=False, default=None, help="Pass shortname hostname value for graphite ingest string to parse. eg. (rcwebsite2)")
      parser.add_argument("-i", "--interval", required=False, default=60, help="manipulate sample interval of data polling. Value in seconds")
      parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose output")

      args = parser.parse_args()

      self.filename = args.file_location
      self.verbose = args.verbose
      self.mds = args.mds
      self.oss = args.oss
      self.hostname = args.hostname
      if self.mds and self.filename==None and self.hostname:
        self.filename ='/proc/fs/lustre/mdt/%s-MDT0000/md_stats' %self.hostname
        print self.filename
      elif self.oss and self.filename==None:
        self.filename ='/usr/sbin/lctl list_param obdfilter.*.stats'
      self.datacenter = args.datacenter
      self.interval = int(args.interval)

      debug = args.verbose
      log_level = logging.INFO
      if debug == True:
        log_level = logging.DEBUG
      if not os.path.isdir(log_location):
        os.makedirs(log_location)
      log_file = "%sgraph_lfs_stats.log" %log_location
      logging.basicConfig(filename=log_file, level=log_level, format=LOG_FORMAT)

      logger.debug(" ".join(sys.argv))
     
  def get_facts(self):
    # running facter -p with json output to capture it into a json file to pull into a dictionary later. 
    facts = {}
    facts = Popen(['facter', '-p', '--json'], stdout=PIPE, stderr=PIPE)
    facts_out,facts_err = facts.communicate()
    if not facts_out and not facts_err:
      print "nada!"
    elif not facts_out:
      print "Um not getting any facts"
    elif facts_err.rstrip():
       print "Error:<<%s>>" %facts_err
    
    mkdir = Popen(['mkdir', '-p', facter_json_location], stdout=PIPE, stderr=PIPE)
    mkdir_out,mkdir_err = mkdir.communicate()

    with open(facter_json_file_location , 'w') as outfile:
      json.dump(facts_out, outfile)

  def dictify_facts(self):
    with open(facter_json_file_location) as facter_file:
      facter_facts = json.load(facter_file)
      json_data = json.loads(facter_facts)
      if self.datacenter == None:
        self.datacenter = json_data['datacenter']
      if self.hostname == None:
        self.hostname = json_data['hostname']

  def get_epoch(self):
    self.epoch_time = int(time.time())
    logger.debug("epoch_time: %s" %self.epoch_time)

  def dictify_mdstat(self):
    if self.filename == None:
      try: 
        mdt_location = 'ls /proc/fs/lustre/mdt/'
        mdt_location = mdt_location.split()
        mdt_cmd = Popen(mdt_location, stdout=PIPE, stderr=PIPE)
        mdt_out, mdt_err = mdt_cmd.communicate()
        if not mdt_out and not mdt_err:
          logger.debug("nada!")
        elif not mdt_out:
          logger.debug("Um not getting any facts")
        elif mdt_err.rstrip():
          logger.debug("Error:<<%s>>" %mdt_err)
        mdt_count = 0
        mdt_list = []
        for mdt in mdt_out.split("\n"): 
          if 'MDT' in mdt:
            mdt_list.append(mdt)
            mdt_count +=1
        if mdt_count = 0:
          sys.exit()
        for mdt in mdt_list:
          self.filename = "/proc/fs/lustre/mdt/%s/md_stats" %mdt
      except OSError, e:
        logger.critical("OSError: %s" %e)
    sample = 1
    while sample <= 2:
      try:
        cat_cmd = "cat /proc/fs/lustre/mdt/%s/md_stats" %mdt
        cat_cmd = cat_cmd.split()
        cat = Popen(cat_cmd, stdout=PIPE, stderr=PIPE)
        cat_out, cat_err = cat_cmd.communicate()
        if not cat_out and not cat_err:
          logger.debug("nada!")
        elif not cat_out:
          logger.debug("Um not getting any facts")
        elif cat_err.rstrip():
          logger.debug("Error:<<%s>>" %cat_err)
      except OSError, e:
        logger.critical("OSError: %s" %e)
      #read the strcture line at a time and build a dict out of it:
      data = {}
      for metric in cat_out.splitlines():
        key = str(metric.split()[0])
        value = int(metric.split()[1])
        data[key] = value
      if sample == 1:
        self.data1 = data
      elif sample == 2:
        self.data2 = data
      sample += 1
      time.sleep(self.interval)
    logger.debug("first data poll: %s" %self.data1)
    logger.debug("Second data poll: %s" %self.data2)

  def dictify_oss_stat(self):
    lctl_cmd = self.filename.split(" ")
    odbfilter = {}
    sample = 1
    while sample <=2:
      try:
        odbfilter = Popen(lctl_cmd, stdout=PIPE, stderr=PIPE)
        odbfilter_out,odbfilter_err = odbfilter.communicate()
        if not odbfilter_out and not odbfilter_err:
          logger.debug("no output or error for lctl command")
        elif not odbfilter_out:
          logger.debug("no output for lctl command")
        elif odbfilter_err.rstrip():
          logger.critical("lclt command error: %s" %odbfilter_err)
        read_io = {}
        write_io = {}
        read_bytes = {}
        write_bytes = {}
        for ost in odbfilter_out.splitlines():
          get_param = "lctl get_param %s" %ost
          #print get_param
          get_param = get_param.split()
          logger.debug("passing ost parms: %s" %get_param)
          ost_stats = {}
          ost_stats = Popen(get_param, stdout=PIPE, stderr=PIPE)
          ost_stat_out, ost_stat_err = ost_stats.communicate()
          ost_name = ost.split('.')[1].split('-')[1]
          for metrics in ost_stat_out.splitlines():
            for metric in metrics.splitlines(): 
              if not metric.find('read_bytes'):
                read_bytes_lst = metric.split()
                key_io = "%s_read_io" %ost_name
                key_bytes = "%s_read_bytes" %ost_name
                read_io[key_io] = read_bytes_lst[1]
                read_bytes[key_bytes] = read_bytes_lst[6]
              if not metric.find('write_bytes'):
                write_bytes_lst = metric.split()
                key_io = "%s_write_io" %ost_name
                key_bytes = "%s_write_bytes" %ost_name
                write_io[key_io] = write_bytes_lst[1] 
                write_bytes[key_bytes] = write_bytes_lst[6]
          if sample == 1: 
            self.read_io = read_io
            self.write_io = write_io
            self.read_bytes = read_bytes
            self.write_bytes = write_bytes
          elif sample == 2:
            self.read_io2 = read_io
            self.write_io2 = write_io
            self.read_bytes2 = read_bytes
            self.write_bytes2 = write_bytes
        sample += 1 
        time.sleep(self.interval)
      except OSError, e:
        logger.critical("OSError: %s" %e)
        
  def dictify_brw(self):
    brw = {}
    heading_list = []
    size = []
    sample = 1
    while sample <=2:
      try:
        brw = Popen(brw_cmd,shell=True, stdout=PIPE, stderr=PIPE)
        brw_out, brw_err = brw.communicate()
        if not brw_out and not brw_err:
          logger.debug("no output or error for lctl command")
        elif not brw_out:
          logger.debug("no output for lctl command")
        elif brw_err.rstrip():
          logger.critical("lclt command error: %s" %brw_err)
        ost_stats = {}
        for line in brw_out.splitlines():
	  if '==>' in line:
	    heading = line.split("/")[5].split("-")[1]+"_"+line.split("/")[6].strip("<==").strip()
          if 'read' in line and 'write' in line:
	    continue
	  if '4K:' in line:
            read_key = heading+'_read_4k'
            write_key = heading+'_write_4k'
	    ost_stats[read_key] = int(line.split()[1]) 
            ost_stats[write_key] = int(line.split()[5])
          if '8K:' in line:
            read_key = heading+'_read_8k'
            write_key = heading+'_write_8k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '16K:' in line:
            read_key = heading+'_read_16k'
            write_key = heading+'_write_16k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '32K:' in line:
            read_key = heading+'_read_32k'
            write_key = heading+'_write_32k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '64K:' in line:
            read_key = heading+'_read_64k'
            write_key = heading+'_write_64k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '128K:' in line:
            read_key = heading+'_read_128k'
            write_key = heading+'_write_128k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '256K:' in line:
            read_key = heading+'_read_256k'
            write_key = heading+'_write_256k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '512K:' in line:
            read_key = heading+'_read_512k'
            write_key = heading+'_write_512k'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
          if '1M:' in line:
            read_key = heading+'_read_1M'
            write_key = heading+'_write_1M'
            ost_stats[read_key] = int(line.split()[1])
            ost_stats[write_key] = int(line.split()[5])
        if sample == 2: 
	  self.ost_stats2 = ost_stats
        else: 
          self.ost_stats1 = ost_stats
          time.sleep(self.interval)
        sample += 1
      except OSError, e:
        logger.critical("OSError: %s" %e)
      
  def get_mds_delta(self):
    self.delta_data={}
    data2 = json.loads(self.data2)
    data1 = json.loads(self.data1)
    for latest_metric, latest_value in data2.iteritems():
      delta_metric = str(latest_metric)
      logger.debug("mds_delta_metric: %s" %delta_metric)
      if delta_metric == 'source' or delta_metric == 'snapshot_time':
        continue
      previous_value = data1[delta_metric]
      #logger.debug("%s - %s / %s "  %(int(latest_value), int(previous_value), self.interval))
      delta_value = (int(latest_value) - int(previous_value))/self.interval
      self.delta_data[delta_metric] = delta_value
    logger.debug("mds_delta_data: %s" %self.delta_data)

  def get_oss_delta(self):
    read_io_delta = {}
    write_io_delta = {}
    read_bytes_delta = {}
    write_bytes_delta = {}
    for key,value in self.read_io2.iteritems():
      pvalue = self.read_io[key]
      delta = (float(value) - float(pvalue))/self.interval
      read_io_delta[key] = delta          
    for key,value in self.write_io2.iteritems():
      pvalue = self.write_io[key]
      delta = (float(value) - float(pvalue))/self.interval
      write_io_delta[key] = delta 
    for key,value in self.read_bytes2.iteritems():
      pvalue = self.read_bytes[key]
      delta = (float(value) - float(pvalue))/self.interval
      read_bytes_delta[key] = delta
    for key,value in self.write_bytes2.iteritems():
      pvalue = self.write_bytes[key]
      delta = (float(value) - float(pvalue))/self.interval
      write_bytes_delta[key] = delta
    self.delta_oss_list.append(read_io_delta) 
    self.delta_oss_list.append(write_io_delta)
    self.delta_oss_list.append(read_bytes_delta)
    self.delta_oss_list.append(write_bytes_delta)
    logger.debug("Delta list of dicts: %s" %self.delta_oss_list)

  def get_oss_brw_delta(self):
    self.delta_brw_data={}
    for latest_metric, latest_value in self.ost_stats2.iteritems():
      previous_value = self.ost_stats1[latest_metric]
      #logger.debug("%s - %s / %s "  %(int(latest_value), int(previous_value), self.interval))
      delta_value = (int(latest_value) - int(previous_value))/self.interval
      self.delta_brw_data[latest_metric] = delta_value
    logger.debug("ost_delta_brw_data: %s" %self.delta_brw_data)
    self.delta_oss_list.append(self.delta_brw_data)

  def push_to_graphite(self):
    logger.debug("MDS delta data output: \n%s" %self.delta_data)
    metrics = self.delta_data
    #metrics = json.loads(self.data)
    content = []
    for metric, value in metrics.iteritems():
      logger.debug("mds value and metrics: %s : %s" %(metric, value))
      metric = str(metric) 
      if metric == 'source':
        continue
      value = float(value)
      #bulding the graphite url
      dots = "."
      params = (graphite_service_name, self.datacenter, self.hostname, metric)
      gurl = dots.join(params)
      logger.debug("parameter strick getting sent to graphite: %s" %gurl)
      data_str = '%s %s %s' %(gurl, value, self.epoch_time)
      content.append(data_str)
    
    content = "\n ".join(content)
    logger.debug("<<< Opening Connection >>>")
    logger.debug("Pushing data: \n %s" %content)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((graphite_server, graphite_port))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    while 1:
      data = s.recv(1024)
      if data == "":
          break
      logger.debug("Recieved: %s" %(repr(data)))
    logger.debug("<<< Connection Closed >>>")
    s.close()

  def push_oss_to_graphite(self):
    content = []
    dots = "."
    #print self.delta_oss_list
    for dicts in self.delta_oss_list:
      for metric, value in dicts.iteritems():
        params = (graphite_service_name, self.datacenter, self.hostname, metric)
        gurl = dots.join(params)
        data = '%s %s %s' %(gurl, value, self.epoch_time) 
        #print data
        if type(data) is UnicodeType:
          #print type(data)
          data = str(data)
          #print data
        content.append(data)  
    content = "\n ".join(content)
    logger.debug("<<< Opening Connection >>>")
    logger.debug("Pushing data: \n %s" %content)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((graphite_server, graphite_port))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    while 1:
      data = s.recv(1024)
      if data == "":
        break
      logger.debug("Recieved: %s" %(repr(data)))
    logger.debug("<<< Connection Closed >>>")
    s.close()
  


if __name__ == '__main__':
  #Global Parameters can be set here:
  ## Log related setting
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  log_location ='/var/log/graphite/'
  logger = logging.getLogger(log_location)
  if not os.path.exists(log_location):
    os.makedirs(log_location)
  ## Use these if you use facter in your environment
  facter_json_file_location = '/var/graphite/facts/facts.json'
  facter_json_location = '/var/graphite/facts'
  ## graphite server settings 
  graphite_server = 'graph.rc.fas.harvard.edu'
  graphite_port = 2003
  graphite_service_name = 'lfs_stats'
  # block read/write command
  # This is quick and dirty, probably a better way to do this:
  brw_cmd = 'tail -n11 /proc/fs/lustre/obdfilter/*/brw_stats'
  # main class
  lfs_stats()
