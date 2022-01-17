### Help to deal with panda job

from pandaclient import panda_api
import argparse
import subprocess
import re
from tty_samples_dict import *
import sys
sys.path.append("/afs/cern.ch/work/b/bmondal/PandaAPI/dicts")
import  MC16a_TOPQ1 as mc16a
import  MC16d_TOPQ1 as mc16d
import  MC16e_TOPQ1 as mc16e

# get key corresponding to value in the sample dictionary
def get_key(dictionary, val):
  for key, values in dictionary.items():
    for value in values:
      if val == value:
        return key

# get campaign from container name
def get_campaign(val):
  split_list = val.split(".") # split the whole string with '.'
  sample_dsid = split_list[1] # dsid
  sample_tags = split_list[len(split_list) -1] # take only e,a,r,p tag
  r_tag = (sample_tags.split("_"))[2] # this only returns AFII/FS 
  if r_tag == "r9364" :
    return "mc16a"
  if r_tag == "r10201" :
    return "mc16d"
  if r_tag == "r10724" :
    return "mc16e"
 

class helpmePanda:
  def __init__(self, tasks):
    self.tasks = tasks
    taskID = 0
    status = ''
    containername=''
    self.done_taskIDs=[]
    self.failed_taskIDs=[]
    self.broken_taskIDs=[]
    self.broken_datasets=[]
    self.running_taskIDs=[]
    self.finished_taskIDs=[]
    for task in self.tasks:
        taskID=task['jeditaskid']
        status=task['status']
        if 'done' in status:
          self.done_taskIDs.append(taskID)
        elif 'failed' in status:
          self.failed_taskIDs.append(taskID)
        elif 'broken' in status:
          self.broken_taskIDs.append(taskID)
          tmp_list = task['datasets'] # get the internal list corresponding to 'datasets'
          tmp_list_first_element = tmp_list[0]
          container_name = tmp_list_first_element['containername']
          self.broken_datasets.append(container_name)
        elif 'running' in status:
          self.running_taskIDs.append(taskID)
        elif 'finished' in status:
          self.finished_taskIDs.append(taskID)

  # get list with datasets name corresponding to broken jobs
  def get_broken_datasets(self):
    return self.broken_datasets

  # create MC16a.py file for broken jobs to resubmit those again
  def create_sample_file_with_broken_jobs(self):
    file_mc16a = open("/afs/cern.ch/work/b/bmondal/PandaAPI/broken_samples/MC16a_TOPQ1.py", 'w')
    file_mc16d = open("/afs/cern.ch/work/b/bmondal/PandaAPI/broken_samples/MC16d_TOPQ1.py", 'w')
    file_mc16e = open("/afs/cern.ch/work/b/bmondal/PandaAPI/broken_samples/MC16e_TOPQ1.py", 'w')
    # write mc16a files
    file_mc16a.write("import TopExamples.grid\n")
    file_mc16a.write("\nsamples = dict()\n")
    # write mc16d files
    file_mc16d.write("import TopExamples.grid\n")
    file_mc16d.write("\nsamples = dict()\n")
    # write mc16e files
    file_mc16e.write("import TopExamples.grid\n")
    file_mc16e.write("\nsamples = dict()\n")
    # list out keys and values separately in samples dict
    for brokends in self.broken_datasets:
      # which campaign it is
      camp = get_campaign(brokends)
      key = ""
      if camp=="mc16a":
        key=get_key(mc16a.samples, brokends)
        file_mc16a.write("samples[\"mc16a_{}\"] = [\'{}\',]\n".format(key, brokends) ) 
      if camp=="mc16d":
        key=get_key(mc16d.samples, brokends)
        file_mc16d.write("samples[\"mc16d_{}\"] = [\'{}\',]\n".format(key, brokends) ) 
      if camp=="mc16e":
        key=get_key(mc16e.samples, brokends)
        file_mc16e.write("samples[\"mc16e_{}\"] = [\'{}\',]\n".format(key, brokends) ) 

    file_mc16a.write("\nfor entry in samples:\n")
    file_mc16a.write("  TopExamples.grid.Add(entry).datasets = [ds for ds in samples[entry]]\n")
    file_mc16d.write("\nfor entry in samples:\n")
    file_mc16d.write("  TopExamples.grid.Add(entry).datasets = [ds for ds in samples[entry]]\n")
    file_mc16e.write("\nfor entry in samples:\n")
    file_mc16e.write("  TopExamples.grid.Add(entry).datasets = [ds for ds in samples[entry]]\n")

    file_mc16a.close()
    file_mc16d.close()
    file_mc16e.close()


  # print how many jobs are in done/failed/broken/running state
  def print_overall_status(self):
    print("==== Done tasks. total number:{} ===\n".format(len(self.done_taskIDs) ) )
    print(self.done_taskIDs)
    print("\n==== Running tasks. total number:{} ===\n".format(len(self.running_taskIDs) ) )
    print(self.running_taskIDs)
    print("\n==== Finished tasks. total number:{} ===\n".format(len(self.finished_taskIDs) ) )
    print(self.finished_taskIDs)
    print("\n==== failed tasks. total number:{} ===\n".format(len(self.failed_taskIDs)) )
    print(self.failed_taskIDs)
    print("\n==== broken tasks. total number:{} ===\n".format(len(self.broken_taskIDs)) )
    print(self.broken_taskIDs)

  # retry failed tasks
  def retry_failed_jobs(self, task):
    sum_list_failed_finished = self.failed_taskIDs + self.finished_taskIDs
    for failed_task in sum_list_failed_finished:
      print(">>>> going to retry task {} <<<<<<<".format(failed_task))
      communication_status, o = c.retry_task(failed_task)
      if communication_status:
          server_return_code, dialog_message = o
          if o == 0:
              print('OK')
          else:
              print ("Not good with {} : {}".format(server_return_code, dialog_message))

  # return list with containername of the done  jobs
  def get_output_container(self, output_filename):
    tmp_list = []
    for task in self.tasks:
      if 'done' or 'finished' or 'running' in task['status']: 
        tmp_list.append(task['taskname'][:-1]+"_"+output_filename+"_root") # cut the last "/" from the container name and add output name
    return tmp_list

  # total size of the done containers
  def get_sizeof_done_containers(self, containername_list):
    tmp_size=0
    print("=== You need to setup rucio first if you haven't done so; setupATLAS and lsetup rucio ===\n")
    print("=== Finding size takes time ")
    #process.stdin.write('setupATLAS; lsetup rucio\n')
    for cont in containername_list:
      process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      process.stdin.write('rucio list-files '+cont+'\n')
      process.stdin.close()
      for line in iter(process.stdout.readline,''):
        if 'size' in line:
          size=re.findall("\d+\.\d+", line)
          if 'GB' in line:
            tmp_size += float(size[0])
          if 'MB' in line:
            tmp_size += float(size[0])/1000

          print("datasetname: {}; {}\n".format(cont, line))
    print ("=== total size of done state samples: {} GB".format(tmp_size))
    if(tmp_size == 0): print ("=== CHECK THAT YOU HAVE SET UP RUCIO. do 'setupATLAS; lsetup rucio' ====")
    


if __name__=="__main__":
  parser = argparse.ArgumentParser(description='This program helps to deal with panda jobs')
  parser.add_argument('--output_filename', type=str, required=True, help="provide the name of the output root file you mentioned in the panda job (name without .root)")
  args = parser.parse_args()

  c = panda_api.get_api()
  tasks = c.get_tasks(days=20)
  helpmePandaObj = helpmePanda(tasks)
  helpmePandaObj.print_overall_status()
  helpmePandaObj.retry_failed_jobs(tasks)
  #print broken datasets
  list = helpmePandaObj.get_broken_datasets()
  #print (list)
  helpmePandaObj.create_sample_file_with_broken_jobs()
  
  #container_list = helpmePandaObj.get_output_container(args.output_filename)
  #print(container_list)
  #helpmePandaObj.get_sizeof_done_containers(container_list)
