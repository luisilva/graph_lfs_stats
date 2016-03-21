#!/usr/bin/python

import sys,os,json,argparse,logging,time,socket
from subprocess import Popen, PIPE
from types import *

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
      self.dictify_oss = self.dictify_oss_stat()
      self.get_oss_delta = self.get_oss_delta()
      self.push_oss_to_graphite = self.push_oss_to_graphite()

  def argparser(self):
      #Setting up parsing options for inputting data
      parser = argparse.ArgumentParser(description="polling lustre for statistics to pump into graphite host")
      parser.add_argument("-m", "--mds", required=False,default=False,action='store_true', help="parsing md_stat on and MDS host")
      parser.add_argument("-o", "--oss", required=False,default=False,action='store_true', help="parsing stats on and OSS host")
      parser.add_argument("-f", "--file-location", required=False, default=None,help="location of mds or oss datafile, default is mds /proc/fs/lustre/mdt/bulfs01-MDT0000/md_stats,\n when oss is enabled odbfilter will be envoked.")
      parser.add_argument("-d", "--datacenter", required=False, default=None, help="Pass datacenter value for graphite ingest string to parse. eg. (holyoke, 1ss, 60ox)")
      parser.add_argument("-n", "--hostname", required=False, default=None, help="Pass shortname hostname value for graphite ingest string to parse. eg. (rcwebsite2)")
      parser.add_argument("-i", "--interval", required=False, default=60, help="manipulate sample interval of data polling. Value in seconds")
      parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose output")

      args = parser.parse_args()

      self.filename = args.file_location
      self.verbose = args.verbose
      self.mds = args.mds
      self.oss = args.oss
      if self.mds and self.filename==None:
        self.filename ='/proc/fs/lustre/mdt/bulfs01-MDT0000/md_stats'
      elif self.oss and self.filename==None:
        self.filename ='lctl list_param obdfilter.*.stats'
      self.datacenter = args.datacenter
      self.hostname = args.hostname
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

  def dictify_mdstat(self):
    sample = 1
    while sample <= 2:
      try:
        f = open(self.filename, 'r')
      except:
        sys.stderr.write("failed to open"+self.filename+"\n")
        sys.exit(-1)
      data = {'source':f.name}
      #read the strcture line at a time and build a dict out of it:
      for line in f:
        words = line.split()
        if(words[-1].isdigit()):
          data[words[0]] = words[-1]
        else:
          data[words[0]] = words[1]
      f.close()
      if sample == 1:
        self.jdata1 = json.JSONEncoder().encode(data)
      elif sample == 2:
        self.jdata2 = json.JSONEncoder().encode(data)
      sample += 1
      time.sleep(self.interval)

  def dictify_oss_stat(self):
    lctl_cmd = self.filename.split(" ")
    odbfilter = {}
    try:
      odbfilter = Popen(lctl_cmd, stdout=PIPE, stderr=PIPE)
    except OSError:
        logger.critical("OSError")
    odbfilter_out,odbfilter_err = odbfilter.communicate()
    if not odbfilter_out and not odbfilter_err:
      print "nada!"
    elif not odbfilter_out:
      print "Um not getting any odbfilter info"
    elif odbfilter_err.rstrip():
      print "Error:<<%s>>" %odbfilter_err

    sample = 1
    while sample <=2:
      # get list of ost's
      read_io = {}
      write_io = {}
      read_bytes = {}
      write_bytes = {}
      for ost in odbfilter_out.splitlines():
        get_param = "lctl get_param %s" %ost
        #print get_param
        get_param = get_param.split()
        #print get_param
        ost_stats = {}
        ost_stats = Popen(get_param, stdout=PIPE, stderr=PIPE)
        ost_stat_out, ost_stat_err = ost_stats.communicate()
        #get ost name 
        ost_name = ost.split('.')[1].split('-')[1]
        for metrics in ost_stat_out.splitlines():
          for metric in metrics.splitlines(): 
            if not metric.find('read_bytes'):
              #print metric
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

  def get_mds_delta(self):
    self.delta_data={}
    jdata2 = json.loads(self.jdata2)
    jdata1 = json.loads(self.jdata1)
    for latest_metric, latest_value in jdata2.iteritems():
      #for previous_metric, previous_value in self.jdata1.iteritems():
      delta_metric = str(latest_metric)
      #print delta_metric
      if delta_metric == 'source' or delta_metric == 'snapshot_time':
        continue
      previous_value = jdata1[delta_metric]
      #print previous_value
      #print "%s - %s / %s "  %(int(latest_value), int(previous_value), self.interval)
      delta_value = (int(latest_value) - int(previous_value))/self.interval
      #print delta_value
      self.delta_data[delta_metric] = delta_value
      #print self.delta_data

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
    self.delta_oss_list = [read_io_delta, write_io_delta, read_bytes_delta, write_bytes_delta]

  def push_to_graphite(self):
    print self.delta_data
    metrics = self.delta_data
    #metrics = json.loads(self.jdata)
    content = []
    for metric, value in metrics.iteritems():
      metric = str(metric) 
      if metric == 'source':
        continue
      value = float(value)
      #print metric, value 
      '''echo "llstat.holyoke.bulfs01mds.open 301 `date +%s`" |nc graph.rc.fas.harvard.edu 2003'''
      #bulding the graphite url
      dots = "."
      params = (graphite_service_name, self.datacenter, self.hostname, metric)
      #print params
      gurl = dots.join(params)
      data_str = '%s %s %s' %(gurl, value, self.epoch_time)
      content.append(data_str)
    
    content = "\n ".join(content)
    print "opening Connection."
    print content
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((graphite_server, graphite_port))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    while 1:
      data = s.recv(1024)
      if data == "":
          break
      print "Received:", repr(data)
    print "Connection closed."
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
    print "opening Connection."
    print content
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((graphite_server, graphite_port))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    while 1:
      data = s.recv(1024)
      if data == "":
        break
      print "Received:", repr(data)
    print "Connection closed."
    s.close()
  


if __name__ == '__main__':
  #Global Parameters can be set here:
  ## Log related setting
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  log_location ='/var/log/graphite/'
  logger = logging.getLogger(log_location)
  ## Use these if you use facter in your environment
  facter_json_file_location = '/var/graphite/facts/facts.json'
  facter_json_location = '/var/graphite/facts'
  ## graphite server settings 
  graphite_server = 'graph.rc.fas.harvard.edu'
  graphite_port = 2003
  graphite_service_name = 'lfs_stats'
  # main class
  lfs_stats()
