#!/usr/bin/python


import sys,os,json,argparse,logging,time,socket
from subprocess import Popen, PIPE

class json_stat:

  def __init__(self):
    self.argparser = self.argparser()
    if not os.path.isfile(facter_json_file_location):
      self.get_facts = self.get_facts()
    if self.datacenter == None or self.hostname == None:
      self.dictify_facts = self.dictify_facts()
    self.get_epoch = self.get_epoch()
    if self.mds:
      self.dictify_mdstat = self.dictify_mdstat()    
    elif self.oss:
      self.dictify_oss = self.dictify_oss_stat()
    self.get_delta = self.get_delta()
    self.push_to_graphite = self.push_to_graphite()

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
        #OST's use formats where the last number is the one you want
        #read_bytes                100121201 samples [bytes] 0 1048576 54023523712987
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
        #OST's use formats where the last number is the one you want
        #read_bytes                100121201 samples [bytes] 0 1048576 54023523712987
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


  def get_delta(self):
    print self.jdata2
    print self.jdata1

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
        self.filename ='lctl get_param obdfilter.*.stats'
      self.datacenter = args.datacenter
      self.hostname = args.hostname
      self.interval = args.interval

      debug = args.verbose
      log_level = logging.INFO
      if debug == True:
        log_level = logging.DEBUG
      if not os.path.isdir(log_location):
        os.makedirs(log_location)
      log_file = "%sgraph_lfs_stats.log" %log_location
      logging.basicConfig(filename=log_file, level=log_level, format=LOG_FORMAT)

      logger.debug(" ".join(sys.argv))

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
    
  def get_epoch(self):
    self.epoch_time = int(time.time())
    #print self.epoch_time
    
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
      #print self.datacenter
      #print self.hostname

if __name__ == '__main__':
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  log_location ='/var/log/graphite/'
  logger = logging.getLogger(log_location)
  facter_json_file_location = '/var/graphite/facts/facts.json'
  facter_json_location = '/var/graphite/facts'
  graphite_server = 'graph.rc.fas.harvard.edu'
  graphite_port = 2003
  graphite_service_name = 'md_stat'
  json_stat()
