### Help to deal with panda job

from pandaclient import panda_api
import argparse
import subprocess
import re

class helpmePanda:
  def __init__(self, tasks):
    self.tasks = tasks
    taskID = 0
    status = ''
    containername=''
    self.done_taskIDs=[]
    self.failed_taskIDs=[]
    self.broken_taskIDs=[]
    self.running_taskIDs=[]
    for task in self.tasks:
        taskID=task['jeditaskid']
        status=task['status']
        if 'done' in status:
          self.done_taskIDs.append(taskID)
        elif 'failed' in status:
          self.failed_taskIDs.append(taskID)
        elif 'broken' in status:
          self.broken_taskIDs.append(taskID)
        elif 'running' in status:
          self.running_taskIDs.append(taskID)


  # print how many jobs are in done/failed/broken/running state
  def print_overall_status(self):
    print("==== Done tasks. total number:{} ===\n".format(len(self.done_taskIDs) ) )
    print(self.done_taskIDs)
    print("\n==== Running tasks. total number:{} ===\n".format(len(self.running_taskIDs) ) )
    print(self.running_taskIDs)
    print("\n==== failed tasks. total number:{} ===\n".format(len(self.failed_taskIDs)) )
    print(self.failed_taskIDs)
    print("\n==== broken tasks. total number:{} ===\n".format(len(self.broken_taskIDs)) )
    print(self.broken_taskIDs)

  # retry failed tasks
  def retry_failed_jobs(self, task):
    for failed_task in failed_taskIDs:
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
      if 'done' in task['status']: 
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
    


if __name__=="__main__":
  parser = argparse.ArgumentParser(description='This program helps to deal with panda jobs')
  parser.add_argument('--output_filename', type=str, required=True, help="provide the name of the output root file you mentioned in the panda job (name without .root)")
  args = parser.parse_args()

  c = panda_api.get_api()
  tasks = c.get_tasks()
  helpmePandaObj = helpmePanda(tasks)
  helpmePandaObj.print_overall_status()
  container_list = helpmePandaObj.get_output_container(args.output_filename)
  print(container_list)
  helpmePandaObj.get_sizeof_done_containers(container_list)
