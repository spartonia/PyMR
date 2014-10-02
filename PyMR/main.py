
import sqlite3 as lite
import json
import sys
from mrjob.job import MRJob
from os import listdir
from Database import * 
from Evaluator import *



class MR(MRJob):
  
  def connect_to(self, db_file):
    self.options.database = db_file

  def configure_options(self):
    super(MR, self).configure_options()
    self.add_file_option('--database', default='test.db')
    self.resultset = {}
    self.audiences = {}
    self.rules = {}
    self.platform = {}
    self.evaluator = Evaluator()



  def mapper_init(self):
    try:
      print "Connecting to the database '{0}' ..".format(self.options.database)
      self.sqlite_conn = lite.connect(self.options.database) 
      self.sqlite_conn.row_factory = lite.Row 
      self.cur = self.sqlite_conn.cursor()
      self.cur.execute("""
                    SELECT A.name, R.condition, R.platform 
                    FROM audience A JOIN audience_rule R ON (A.id = R.audience_id)
                    -- WHERE R.id = 1
                    """)
      self.resultset = self.cur.fetchall()
      for idx, item in enumerate(self.resultset):
        self.rules[idx] = item['condition']
        self.audiences[idx] = item['name'] 
        self.platform[idx] = item['platform']
      print "Connected!"  
    except:
      print "Connetion failed!"
      print "Exiting.."
      sys.exit()
    
  def mapper(self, _, line):

    try:
      event = json.loads(line)
      if event["TMEvent"] and int(event["TMEvent"]) == 24:
        for idx in range(len(self.rules)):
          # if condition is satisfied  
          if ( self.evaluator.evaluate(self.rules[idx], event, self.platform[idx]) ):
            
            yield (event["installID"], self.audiences[idx])  
            # yield (self.audiences[idx], event["installID"])
          else:
            yield (event["installID"],_)
            # yield ("nothing",event["installID"])

    except:
      # yield ("error", 1)
      pass

  def combiner(self, key, value):
    combinedList = []
    for val in value:
      if val:
        if val not in combinedList and val != 'None':
          combinedList.append(val)
    yield (key, combinedList)


  def reducer(self, key, values):
    
    audList = []
    for value in values:
      for val in value:
        if val not in audList:
          audList.append(val)
    yield (key, audList)
  
def main(args):
  
  if len(args) < 2:
    print "*** Enter input file name for MRJob!" 
    print "*** Usage: python <file.py> <logname> [db_name]"
    return

  # log = "logstream.log.analytics-prod-eu-front-4.20131108204001.gz"
    
  if len(args) == 2:
    Database()
    mr_job = MR([args[1]])

  else:
    db_name = args[2]
    if db_name not in listdir('.'):
      print "Database '{0}' does not exist! Creating a new one..".format(db_name) 
      Database(db_name)
    DBparam = "--database={0}".format(db_name)
    mr_job = MR([args[1] ,DBparam])

  with mr_job.make_runner() as runner:
    runner.run()
    print '*' * 40
    print "Starting MRJob.."
    matched = 0
    not_matched = 0
    for line in runner.stream_output():
      key, value = mr_job.parse_output_line(line)
      if not value:
        not_matched +=1
        print key, "-"
      else:
        matched +=1
        print key, value
    print "Matched records: ", matched
    print "Records not matched: ", not_matched

# 
if __name__ == '__main__':
  
  main(sys.argv)

  


