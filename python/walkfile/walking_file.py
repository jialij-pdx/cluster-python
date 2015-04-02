# Cluster-walking files
# Copyright (C) 2015  Jiali Ju

import os
import subprocess
import timeit


# the original data archive
#root = '/Users/personal/Documents/research/useful'
root ="/home/jialij/walkfile/Data_BPA/Data_B_PMUs/2014/140410"

# the destination folder directory
storage_root = "/home/jialij/walkfile/Temp"


# filepath json dictionary
file_path = {}


# walk through files
def walk_file(root):
    # walk the directory
    full_path = []

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
                    run_R(filepath, new_dir, names)

        # file path dictionary
        file_path = {"file_path": full_path}



# run R script in Python
# (after anonymizing: anonymizer will copy the file over to the destination folder)
# parse the destination folder to the anonymizer
def run_R(path,dest_path, filename):

    failed_filelist = []
    # find R script in current working directory
    #script_R = os.path.join(os.getcwd(), 'test.R')
    script_R = "/home/jialij/walkfile/bpapmu2014/bin/pdat_hash"

    # three arguments: script, destination directory, and filename
    dest = os.path.join(dest_path, filename)

    start_time = timeit.default_timer()

    try:
        subprocess.check_call([script_R, '-s', path, '-d', dest])
    except subprocess.CalledProcessError as e:
        print e.output
        failed_filelist.append(path)

    end_time = timeit.default_timer()

    print "Total: ", end_time - start_time, " long"


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


if __name__ == '__main__':

    start_time = timeit.default_timer()
    walk_file(root)
    end_time = timeit.default_timer()

    print "Total: ", end_time - start_time, " spent"
