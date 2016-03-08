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

import sys,json

class json_stat:

  def __init__(self):
    self.get_file_path = self.get_file_path()
    self.dictify = self.dictify()    
    self.push_to_graphite = self.push_to_graphite()

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
    #json.JSONEncoder().iterencode(data)
    #self.data

  def get_file_path(self):
    for x in range(1, len(sys.argv)):
      self.filename = sys.argv[x]
      #dictify(filename)

  def push_to_graphite(self):
    print self.jdata

if __name__ == '__main__':
  json_stat() 
