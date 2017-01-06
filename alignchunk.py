import luigi
import pandas as pd
from flatfieldcorrection import flatfieldcorrection
import os
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
#sys.path.insert(0,'/nas/data/M239923_RorBTdt_2/scripts/')
sys.path.insert(0,os.getcwd())
from celery import Celery
from tasks import run_celerycommand
from tasks import run_alignment
from stitching import stitch_section
from stitch_ribbon import stitch_ribbon_session_channel
from checkfileexists import checkfileexists
import time

class alignchunk(luigi.Task):
 
   chunk = luigi.Parameter()

   def output(self):
	filename = '../processed/%s/project.xml'%(self.chunk)        		
	return luigi.LocalTarget(filename)
	#return luigi.LocalTarget("statetable")
	#return checkfileexists(filename)
   
   def complete(self):
	filename = '../processed/jobs/align-%s.log'%(self.chunk)        		
	print(filename)

	if os.path.exists(filename):
		line = os.popen("tail -n 1 %s" % filename).read()
		print line
		words = line.split()
		print words
		if words[0]=="After":
			return True
		else:
			return False
	else:
		return False

   def requires(self):
	filename = '../processed/jobs/align-%s'%(self.chunk) 
	return checkfileexists(filename)
		
	
   def run(self):
	print "NOW RUUNINGGGGGGGGGGGGGG %s" % (self.chunk)
	mydir = os.getcwd() + '/../processed/jobs/'
	myfile = mydir + 'align-%s'%(self.chunk)
	
	result = run_alignment.apply_async(args=[myfile,mydir], countdown=3)
	time.sleep(10)
			
