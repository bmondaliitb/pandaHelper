from helpmePanda import helpmePanda
from pandaclient import panda_api
import sys
from tty_samples_dict import *
import re
import subprocess
import argparse

debug = True

def get_dict_dsid_atag_foldername():
  # dictionary of dsid, a_tag, foldername
  dict_dsid_atag_foldername={}
  
  for entry in samples:
    for sample in samples[entry]:
      split_list = sample.split(".") # split the whole string with '.'
      sample_dsid = split_list[1] # dsid
      sample_tags = split_list[5] # take only e,a,r,p tag
      a_tag = (sample_tags.split("_"))[1] # this only returns AFII/FS 
      #print("{}  {} {} \n".format(sample_dsid, a_tag[0], entry))
      dict_dsid_atag_foldername[(int(sample_dsid), str(a_tag[0]) )] = entry
   
  return dict_dsid_atag_foldername

def download_files_from_grid(container_list, dictionary, output_path):
  for container in container_list:
    if debug: print(container)
    cont_split = container.split('.')
    cont_dsid = int(cont_split[2])
    cont_tags = ((cont_split[len(cont_split) -3]).split('_'))
    cont_atag = cont_tags[1]
    if debug:print("{}   {} ".format(cont_dsid, str(cont_atag[0]) ))
    folder_name = dictionary[cont_dsid, str(cont_atag[0]) ]
    if debug: print(" Folder name: {} ".format(folder_name))
    # use subprocess to download 
    process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.stdin.write('cd {}; mkdir -p {}; pushd {} \n'.format(output_path, folder_name, folder_name))
    process.stdin.write('rucio download '+container+'\n')
    process.stdin.write('popd \n')
    process.stdin.close()
    if debug: 
      for line in iter(process.stdout.readline,''):
        print(line)
    
     

if __name__=="__main__":
  parser = argparse.ArgumentParser(description='This program helps to deal with panda jobs')
  parser.add_argument('--job_output_filename', type=str, required=True, help="provide the name of the output root file you mentioned in the panda job (name without .root)")
  parser.add_argument('--output_path', type=str, required=True, help="ABSOLUTE path where you want to download the files; please use absolute path; folder structure will automatically be created")
  args = parser.parse_args()

  output_filename = args.job_output_filename#'output_ttgamma'
  output_path = args.output_path#'/afs/cern.ch/work/b/bmondal/PandaAPI/test_path'
  dictionary = get_dict_dsid_atag_foldername()
  c = panda_api.get_api()
  tasks = c.get_tasks()
  helpmePandaObj = helpmePanda(tasks)
  helpmePandaObj.print_overall_status()
  container_list = helpmePandaObj.get_output_container(output_filename)
  download_files_from_grid(container_list, dictionary, output_path)
