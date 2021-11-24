### Help to deal with panda job

from pandaclient import panda_api
c = panda_api.get_api()

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
        #print ('taskID={} status={} containername={}'.format(task['jeditaskid'], task['status'], task['datasets']))
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

  # print containername of the done and running jobs
  def get_output_container(self):
    tmp_list = []
    for task in self.tasks:
      if 'done' in task['status']: 
        tmp_list.append((task['datasets'][1])['containername'])
    return tmp_list

    

if __name__=="__main__":
  tasks = c.get_tasks()
  helpmePandaObj = helpmePanda(tasks)
  helpmePandaObj.print_overall_status()
  print(len(helpmePandaObj.get_output_container()))
  for container in  helpmePandaObj.get_output_container():
    print(container)
    print("\n")

