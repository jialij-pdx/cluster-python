# Cluster-walking files
# Copyright (C) 2015  Jiali Ju

import os, sys
import subprocess
import timeit
import datetime
import traceback


# the original data archive
#root = '/Users/personal/Documents/research/useful'
#root ="/scratch2/tshott/data"
root = "/scratch2/tshott/jialij_test/BPA/WISPDitt/2014/140410"


# the destination folder directory
storage_root = "/scratch2/tshott/jialij_test/Generic"

# log file directory
log_root = "/scratch2/tshott/jialij_test/log"


# filepath json dictionary
file_path = {}
# output from R script
output = ''

# walk through files
def walk_file(root):
    # walk the directory
    full_path = []
    failed_filelist = []

    # check if empty directory, stop
    if not os.path.exists(root):
        print "The data archive does not exist. "
    else:
        for path, dir, files in os.walk(root, topdown=False):

            for names in files:
                filepath = os.path.join(path, names)

                if 'pdat' in os.path.basename(names).split('.'):
                    print "filename is: ", names

                    source_path = os.path.join(root,names)
                    new_dir =  get_new_directory(filepath)
                    print new_dir

                    #if not os.path.exists(new_dir):
                    #    os.makedirs(new_dir)

                    # run R script for each file
                    failedpath = run_R(source_path, new_dir, names)

                    # add failed directories to the list
                    if not failedpath:
                        failed_filelist.append(failedpath)

    if not failed_filelist:
        print failed_filelist



    """
    # check if empty directory, stop
    if not os.path.exists(root):
        print "The data archive does not exist. "
    else:
        for path, dir, files in os.walk(root, topdown=False):

            for names in files:
                filepath = os.path.join(path, names)
                if 'pdat' in os.path.basename(filepath).split('.'):
                    print "filename is: ", names
                    full_path.append(filepath)
                    new_dir =  get_new_directory(filepath)

                    # run R script for each file
                    failedpath = run_R(filepath, new_dir, names)
                    
                    # add failed directories to the list
                    if not failedpath: 
                        failed_filelist.append(failedpath)
        # file path dictionary
        file_path = {"file_path": full_path}
        
        if not failed_filelist:
            print failed_filelist

        """

# run R script in Python
# (after anonymizing: anonymizer will copy the file over to the destination folder)
# parse the destination folder to the anonymizer
def run_R(path,dest_path, filename):
	
    failed_path = ''
    # find R script in current working directory
    #script_R = os.path.join(os.getcwd(), 'test.R')
    script_R = "/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_hash"

    # three arguments: script, destination directory, and filename
    dest = os.path.join(dest_path, filename)
    #dest = dest_path

    start_time = timeit.default_timer()

    start_process_time = datetime.datetime.now()

    # capture output to log file
    #date_dir = path.split('/')[-2]
    log_path = os.path.join(log_root, current_date, filename)
    new_dir = os.path.dirname(log_path)
    print new_dir
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    log_name = str(start_process_time) + '_output.txt'
    log_output = open(os.path.join(new_dir, log_name),'w')
    log_output.write("Log: " + str(start_process_time) + "\n")

    # start process
    global pymsg
    try:
        subprocess.check_call([script_R, '-s', path, '-d', dest])

    except subprocess.CalledProcessError as e:
        print "Error: ", e
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
   
        pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        failed_path = path

    end_time = timeit.default_timer()

    print "Total: ", end_time - start_time, " long"

    print "+" * 20
    #print "output: ", output
    
    if not pymsg:
        pymsg = "Complete"
    else:
        log_output.write(pymsg)
    log_output.close()

    return failed_path


# generate new directory according to the orginal name
def get_new_directory(path):

    file_name, file_path = get_filepath(path)
    date_name, date_path = get_filepath(file_path)
    year_name, year_path = get_filepath(date_path)
    type_name, type_path = get_filepath(year_path)

    new_full_path = os.path.join(storage_root,type_name, year_name,date_name,file_name)

    # check if the directory exits, Not - create one
    new_dir = os.path.dirname(new_full_path)
    print new_dir
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

    return new_dir

# get file's basename and full path
def get_filepath(path):
    # levels: PMU type - Year - Date - files
    file_name = os.path.basename(path)
    parent_path = os.path.dirname(path)

    return file_name, parent_path

def get_date():
    current = datetime.datetime.now()
    year = current.year
    month = current.month
    day = current.day

    date_identifier = str(year)+ "%02d" % month + "%02d" % day
    return date_identifier

if __name__ == '__main__':

    # get current date time
    global current_date
    current_date = get_date()

    start_time = timeit.default_timer()
    walk_file(root)
    end_time = timeit.default_timer()

    print "Total: ", end_time - start_time, " spent"
