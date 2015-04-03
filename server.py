"""
1. specify time range
2. copy files within the time range to tmp folder
3. every time take out <= 60 files as 1 hour chunk from tmp folder
4. run script on these files
5. get the result(store it?) and delete files in /tmp
"""
import sys,time,timeit
import os,datetime
import subprocess
import shutil

"""
usr menu, generate job list
"""
# real root
#root = '/scratch2/tshott/BPA_Generic'

#for test purpose
root = '/scratch2/tshott/BPA_Generic_test/tshott/BPA_data_test/141105'


script_root = '/home/jialij/walkfile/rtest2/bpapmu2014/bin'

storage_root = '/scratch2/tshott/result/test'
#storage_root = '/scratch2/tshott/result'

temp_folder = '/scratch2/tshott/tmp/test'
#temp_folder = '/scratch2/tshott/tmp'

# initialize job array
job_input = []


# get job list
def get_job():
    # input menu
    menu = {'1': 'Please enter the start time in YYYY-MM-DD HH:MM:DD format',
            '2': 'Please enter the end time in YYYY-MM-DD HH:MM:DD format, Default is one hour later',
            '3': 'What type of data? (Enter A, B, Partner, or Wind)',
            '4': 'Where is your analysis script?'}

    for key in sorted(menu):
        print key, ' : ', menu[key]

    count = 0
    
    # ask Users to submit jobs, end input when Users say no. 
    while True:
        user_job = raw_input('Do you want to submit a job? > Y if yes, N if no \n')
        if user_job == 'Y' or user_job == 'y' or user_job == 'yes':

            #default input
            user_start_time = '2014-11-05 00:00:00'
            #user_start_time = '2014-09-16 00:00:00'
            start_time = format_time(user_start_time)
            user_end_time = str(start_time + datetime.timedelta(hours=1))
            
            user_type = 'A'
            user_script = "/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_check"

            # User input: start time, end time, pmu type, and the analysis script
            # For each job, ask for users input until they say done.
            while True:
                user_input = raw_input('Please choose from menu: 1/2/3/4, enter D if you are done. \n')
                if user_input == '1':
                    # need time value check, if not pattern, try again

                    user_start_time = raw_input(menu['1'] + '\n')
                elif user_input == '2':
                    # need time value check, if not pattern, try again

                    user_end_time = raw_input(menu['2'] + '\n')
                elif user_input == '3':
                    # need type value check, if not pattern, try again

                    user_type = raw_input(menu['3'] + '\n')
                elif user_input == '4':
                    # need shell cmd check, if not pattern, try again

                    #user_script = raw_input(menu['4'] + '\n')
                    print menu['4']

                    script_file = raw_input('Do you already have a analysis? Y or N \n')
                    if script_file in ['Y', 'y', 'Yes', 'yes', 'YES']:
                        #user_script = raw_input('Please provide the file path')

                        user_filename = raw_input('Please enter a new script name \n')
                        #subprocess.call(['nano', user_filename])

                        user_script = os.path.join(script_root, user_filename)

                    # check if it's an empty file
                    if not os.path.getsize(user_script):
                        print "no script, please make sure you add the analysis file. \n"

                elif user_input == 'D' or user_input == 'd' or user_input == 'done':
                    break
                    
            count += 1
            #print user_start_time, user_end_time, user_type, user_script

            # add to job array
            #job_input.append([count, user_start_time, user_end_time, user_type, user_script])
            #print job_input
            
            # get the file path
            search_file_list = get_search_fileList(user_start_time, user_end_time, user_type)
            print search_file_list

            job_input.append([count, user_start_time, user_end_time, user_type, user_script, search_file_list])
        else:
            break

    print job_input
    return job_input

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


# run job
def run_job():
    job = get_job()

    while True:
        try:
            ready_job = job[0]
            job.pop(0)
        except IndexError:
            print 'job all done'
            break

        #job array: 0-id, 1-start time, 2-end time, 3-type, 4-script, 5-search_file_list
        source_dir = ready_job[-1][0]
        print source_dir

        script_dir = ready_job[4]
        job_id = ready_job[0]
        start_time_wanted = ready_job[1]


        print "chunk: all"
        chunk = raw_input("Yes or No?")
        chunk_size = ''
        if chunk in ['No', 'N']:
            chunk_size = raw_input("Please enter chunk size: how many files?")

        for src_dir in source_dir:
            #print src_dir
            copy_file(src_dir,temp_folder,job_id, start_time_wanted)
            run_R(job_id,src_dir,script_dir, chunk,chunk_size)

# copy file to the temp folder
def copy_file(src_dir,dest_dir,job_id, start_time):

    new_folder = str(job_id) + '_'+ time_to_name(start_time)
    dest = dest_dir + '/'+ new_folder
    if not os.path.exists(dest):
        os.makedirs(dest)
    try:
        shutil.copy(src_dir,dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)

# run R script in Python
# store the output in the result folder
def run_R(job_id, src_dir, script_dir,chunk, chunk_size):

    failed_path = ''
    script_R = script_dir
    source = os.path.dirname(src_dir)
    filename = os.path.basename(src_dir)
    #dest_result = dest_dir

    # required argument: -d, source dir
    # optional: -p, default as generic data; -c chnck size

    start_time = timeit.default_timer()

    try:


        # generate output filename

        file_name = str(job_id)+'_'+ os.path.splitext(filename)[0]+'_output.txt'
        print file_name

        dest = os.path.join(storage_root, file_name)
        with open(dest, "w+") as output:
            print "trying to write files"
            if chunk_size != '':
                subprocess.check_call([script_R, '-d', source, '-c', int(chunk_size)],stdout=output)
            else:
                #"hello world"
                subprocess.check_call([script_R, '-d', source],stdout=output)
                #subprocess.check_call([script_R, '-d', source, '-p', 'BPA'],stdout=output)
    except subprocess.CalledProcessError as e:
        print e
        failed_path = source

        output.close()

    end_time = timeit.default_timer()

    print "Total: ", end_time - start_time, " long"
    return failed_path

    """
    print >>sys.stderr, check
    if check == 'Done':
        print >>sys.stderr, 'job success'
    else:
        print >>sys.stderr, 'need to re-do'
        job.insert(0,ready_job) #put back to the top of list
    """

if __name__ == '__main__':

    start_time = timeit.default_timer()
    run_job()
    end_time = timeit.default_timer()

    print "Total: ", (end_time - start_time)/60, " long"

    print 'All done'
