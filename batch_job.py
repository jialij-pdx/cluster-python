"""
generate the file list to run the script by time range
"""
import sys,time,timeit
import os,datetime
import subprocess
import shutil
import logging

"""
generate job list by given time range
"""
# real root
#root = '/scratch2/tshott/BPA_Generic'

#for test purpose
root = '/scratch2/tshott/jialij_test/BPA'
#root = '/Users/personal/documents/tshott/BPA_data_test/WISPDitt/2014/140410'


script_root = '/home/jialij/walkfile/rtest2/bpapmu2014/bin'

storage_root = '/scratch2/tshott/jialij_test/Generic'
#storage_root = '/scratch2/tshott/result'

temp_folder = '/scratch2/tshott/tmp/test'
#temp_folder = '/scratch2/tshott/tmp'

# initialize job array
job_input = []


# get job list
def get_job():

    #default input
    user_start_time = '2014-11-05 00:00:00'
    #user_start_time = '2014-09-16 00:00:00'
    start_time = format_time(user_start_time)

    user_end_time = str(start_time + datetime.timedelta(hours=1))

    user_type = 'A'

    #user_script = "/Users/personal/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_check"
    user_script = "/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_hash_file"

    user_start_time = raw_input('Please enter the start time in YYYY-MM-DD HH:MM:DD format' + '\n')
    user_end_time = raw_input('Please enter the end time in YYYY-MM-DD HH:MM:DD format' + '\n')

    # get the file path
    search_file_list = get_search_fileList(user_start_time, user_end_time, user_type)
    print search_file_list

    return search_file_list

"""
Search for the file list according to user input
"""

# format time to YYYY-MM-DD HH:MM:SS
def format_time(date_time):
    format_time = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
    return format_time

# convert time string to file name
def time_to_name(time):
    time = time.replace('-','')
    time = time.replace(':','')
    time_name = time.replace(' ','_')
    return time_name

# search files by given time range and type, add file list to job array
# after format checking
def search_file_day(start_time, end_time, type):
    search_list = []
    type_dic = {'A': 'WISPDitt', 'B': 'WISPMunro', 'Wind': 'WISPWind', 'Partner': 'WISPPartner'}
    pmu_type = type_dic[type]

    start_time_name = time_to_name(start_time)
    end_time_name = time_to_name(end_time)

    # generate filename
    start_filename = '_'.join((pmu_type, start_time_name)) + '.pdat'
    end_filename = '_'.join((pmu_type, end_time_name)) + '.pdat'

    print start_filename
    print end_filename

    # generate the require file list

    # walk the directory  TODO: empty directory check
    for path, dir, files in os.walk(root, topdown=False):
        for i in range(len(files)):
            #print files[i]
            if files[i] == start_filename:
                search_list.append(os.path.join(path,files[i]))
                while True:
                    i += 1
                    try:
                        #print 'next file is: ', files[i], '\n'
                        filepath = os.path.join(path, files[i]) # get the full path of the file
                        if files[i].split('.')[-1] != 'pdat':
                            os.remove(filepath)
                        search_list.append(filepath)
                        if files[i] == end_filename:
                            #print 'find all files'
                            break
                    except IndexError:
                        break
    return search_list

# calculate it's same day or different day
def get_day_list(start_time, end_time):
    day_list = []

    start_day = format_time(start_time).day
    end_day = format_time(end_time).day

    delta = end_day - start_day
    if delta > 0: # more than 1 day
        day_range = range(start_day, end_day+1)
        for i in range(len(day_range)):
            # add day_end for start_time
            if i == 0:
                day_end = end_of_day_time(start_time)
                day_list.append((start_time, day_end))

            elif i == len(day_range) - 1: # the last index
                day_start = start_of_day_time(end_time) # add start_time
                day_list.append((day_start, end_time))

            else:  # add start time and end time for days in between
                day_start, day_end = overnight_day_time(start_time, i)
                day_list.append((day_start, day_end))

    else:  # the same day
        day_list.append((start_time, end_time))

    return day_list

# add end of day time
def end_of_day_time(start_time):
    end_of_day = str(start_time).split(' ')[0]+ ' 23:59:59'
    return end_of_day

# add start of day time
def start_of_day_time(end_time):
    start_of_day = str(end_time).split(' ')[0] + ' 00:00:00'
    return start_of_day

# add full start of day and end of day time
def overnight_day_time(start_time, timedelta):
    start_day = str(format_time(start_time) + datetime.timedelta(timedelta)).split(' ')[0] + ' 00:00:00'
    end_day = str(format_time(start_time) + datetime.timedelta(timedelta)).split(' ')[0] + ' 23:59:59'
    return start_day, end_day
    
def get_search_fileList(start_time, end_time, type):
    fileList = []
    day_list = get_day_list(start_time, end_time)
    for l in day_list:
        usr_start, usr_end = l
        day_file_list = search_file_day(usr_start, usr_end, type)
        fileList.append(day_file_list)
    return fileList

# generate batch job queue
def batch_job():
    file_list = get_job()
    job_queue = []

    user_script = "/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_hash_file"

    count = 1
    # root + WISPDitt + Year + Day + filename
    for f in file_list[0]:
        dest = dest_generate(f)

        # batch_job: job_id, src_path, dest_path, script
        job_queue.append([count,f, dest, user_script])
        count += 1

    print job_queue
    return job_queue

def dest_generate(path):

    # root + WISPDitt + Year + Day + filename
    copy_dir = path.split('/')[-4:-2]
    filename = path.split('/')[-1]

    dir_string = '/'.join(copy_dir)

    # create the destination directory
    new_dir = os.path.join(storage_root, dir_string)

    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

    # create the full path for the result file
    new_filepath = os.path.join(new_dir,filename)

    return new_filepath


if __name__ == '__main__':

    start_time = timeit.default_timer()
    #run_job()

    batch_job()

    end_time = timeit.default_timer()

    print "Total: ", (end_time - start_time)/60, " long"

    print 'All done'

