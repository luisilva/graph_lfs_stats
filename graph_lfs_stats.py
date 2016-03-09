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

import sys,os,json,argparse,logging,time

class json_stat:

  def __init__(self):
    self.argparser = self.argparser()
    self.dictify = self.dictify()    
    self.push_to_graphite = self.push_to_graphite()
    self.get_facts = self.get_facts()
    self.get_epoch_time = get_epoch_time()

  def dictify(self):
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

  def get_facts(self):
    facts = {}
    for fact in os.popen("facter -p").readlines():
      try:
        n, v = fact.split(" => ")
        print "%s %s" %(n,v)
        facts[ n ] = v.rstrip()
      except:
	print "hit the baddy: %s" %(facts) 
    
    for x in facts:
      print (x)
    for y in facts[x]:
        print (y,':',facts[x][y])
    
    print facts['dataceter']
    # 
    #f = facter.Facter() 
    #self.datacenter = f["datacenter"].lower()
    #self.hostname = f["hostname"].lower()
    
  def get_epoch_time():
    self.epoch_time = time.time()
    #print self.epoch_time
    
if __name__ == '__main__':
  LOG_FORMAT = "[%(asctime)s][%(levelname)s] - %(name)s - %(message)s"
  logger = logging.getLogger('/var/log/graphite/graph_lfs_stats.log')
  json_stat()
