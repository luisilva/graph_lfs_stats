#!/usr/bin/python

'''
Takes data in the form of:
metric         number
snapshot_time             1396141904.951010 secs.usecs
open                      28540765918 samples [reqs]
close                     10256936166 samples [reqs]
mknod                     30061 samples [reqs]
and creates a json version:
metic: number
'''

import sys,os,json,argparse,logging,time,shlex,socket
from subprocess import Popen, PIPE

class json_stat:

  def __init__(self):
    self.argparser = self.argparser()
    if not os.path.isfile(facter_json_file_location):
      self.get_facts = self.get_facts() 
    self.dictify_facts = self.dictify_facts()
    self.get_epoch = self.get_epoch()
    self.dictify_mdstat = self.dictify_mdstat()    
    self.push_to_graphite = self.push_to_graphite()

  def dictify_mdstat(self):
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
    self.jdata = json.JSONEncoder().encode(data)

  def argparser(self):
      #Setting up parsing options for inputting data
      parser = argparse.ArgumentParser(description="polling lustre for statistics to pump into graphite host")
      parser.add_argument("-m", "--mdt", required=False,default=True, help="parsing md_stat on and MDS host")
      parser.add_argument("-o", "--ost", required=False, help="parsing md_stat on and MDS host")
      parser.add_argument("-f", "--file-location", required=False, default="/proc/fs/lustre/mdt/bulfs01-MDT0000/md_stats",help="location of mdt or ost datafile, default is mdt /proc/fs/lustre/mdt/bulfs01-MDT0000/md_stats")
      parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose output")

      args = parser.parse_args()

      self.filename = args.file_location
      self.verbose = args.verbose
      self.mdt = args.mdt
      self.ost = args.ost

      debug = args.verbose
      log_level = logging.INFO
      if debug == True:
        log_level = logging.DEBUG
      logging.basicConfig(filename="/var/log/graphite/graph_lfs_stats.log", level=log_level, format=LOG_FORMAT)

      logger.debug(" ".join(sys.argv))

  def push_to_graphite(self):
    #print self.jdata
    metrics = json.loads(self.jdata)
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
      self.datacenter = json_data['datacenter']
      self.hostname = json_data['hostname']
      #print self.datacenter
      #print self.hostname

if __name__ == '__main__':
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  logger = logging.getLogger('/var/log/graphite/graph_lfs_stats.log')
  facter_json_file_location = '/root/graphite/facts.json'
  facter_json_location = '/root/graphite'
  graphite_server = 'graph.rc.fas.harvard.edu'
  graphite_port = 2003
  graphite_service_name = 'md_stat'
  json_stat()
