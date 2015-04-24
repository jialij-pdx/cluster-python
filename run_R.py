#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2008 Doug Hellmann All rights reserved.
#
"""create workers to run R script on each file
   get src, dest, and script file from batch_job script
"""
#end_pymotw_header

import multiprocessing
import timeit
import subprocess
from batch_job import batch_job
import logging

def worker(i,src,dest,script_dir):
    """worker function"""
    failed_path = ""
    #script_dir = "/home/jialij/walkfile/rtest2/bpapmu2014/bin/pdat_hash"
    #src_dir = "/scratch2/tshott/BPA/WISPDitt/2014/141104/WISPDitt_20141104_230000.pdat"
    #dest_dir = "/scratch2/tshott/jialij_test/Generic/WISPDitt_20141104_230000.pdat"

    print "Worker: ", i
    src_dir = src
    dest_dir = dest
    start_time = timeit.default_timer()

    try:
       
        subprocess.check_call([script_dir, '-s', src_dir, '-d', dest_dir])

    except subprocess.CalledProcessError as e:
        print e
        failed_path = src_dir


    end_time = timeit.default_timer()

    logging.info("spending %s long", (end_time - start_time))

    print "Total: ", end_time - start_time, " long"
    return failed_path


if __name__ == '__main__':

    logging.basicConfig(filename = 'mytest.out', format='%(asctime)s %(message)s',level=logging.DEBUG)
    logging.info('start')
    jobs = []
    myjob = batch_job()
    logging.info(myjob)
    #print "file_list: ", myjob
    #myjob = [[1,"/scratch2/tshott/BPA/WISPDitt/2014/141104/WISPDitt_20141104_230000.pdat","/scratch2/tshott/jialij_test/Generic/WISPDitt_20141104_230000.pdat"],[2,"/scratch2/tshott/BPA/WISPDitt/2014/141104/WISPDitt_20141104_230100.pdat", "/scratch2/tshott/jialij_test/Generic/WISPDitt_20141104_230100.pdat"],[3,"/scratch2/tshott/BPA/WISPDitt/2014/141104/WISPDitt_20141104_230200.pdat","/scratch2/tshott/jialij_test/Generic/WISPDitt_20141104_230200.pdat"]]
    for list in myjob:
        id = list[0]
        #print "job: ", id
        src = list[1]
        #print "src: ", src
        dest = list[2]
        #print "dest: ", dest
        script = list[3]

        p = multiprocessing.Process(target=worker, args=(id,src,dest,script))
        jobs.append(p)
        p.start()
    logging.info('done')
