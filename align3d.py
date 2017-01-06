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
from stitching import stitch_section
from stitch_ribbon import stitch_ribbon_session_channel
from checkfileexists import checkfileexists
import glob
from alignchunk import alignchunk
from run_alignment_setup import run_alignment_setup

class checkallfilesexist(luigi.Task):
    
    df = luigi.Parameter()

    def output(self):
       	uniq_channel_sessions = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
	for (rib,sess,sect,chan) in uniq_channel_sessions:	
		if ((sess == 0 ) & (chan == 0)) :
			R.append(rib)
			S.append(sess)
	return [luigi.LocalTarget('../processed/stitched_files/rib%04dsess0000sect%04d_DAPI.tif'%(R[index], S[index])) for index in range(0, len(R))]	
    def run(self):
	print "Checking is all files in the block exist"
    


class alignallchunks(luigi.Task):
    chunks = luigi.Parameter()
    df = luigi.Parameter()

    def output(self):
	return [luigi.LocalTarget('../processed/%s/project.xml'%(self.chunks[index])  ) for index in range(0, len(self.chunks)) ]


    def requires(self):
	return [alignchunk(self.chunks[index]) for index in range(0, len(self.chunks))]

    def run(self):
	print "Aligning all chunks"


class align3d(luigi.Task):
   
   df=pd.read_csv("statetable")	

   def output(self):
	dirnames = glob.glob('../processed/*-*')
	chunknames = []	
	for index in range (0, len(dirnames)):
		[left,sep,chunkname] = dirnames[index].rpartition('/')
		chunknames.append(chunkname)
		print chunkname
	return [luigi.LocalTarget("../processed/%s/project.xml"%chunknames[index]) for index in range(0, len(chunknames))]

   def complete(self):

        dirnames = glob.glob('../processed/*-*')
	chunknames = []
	
	for index in range (0, len(dirnames)):
		[left,sep,chunkname] = dirnames[index].rpartition('/')
		chunknames.append(chunkname)
		print chunkname

	for index in range(0,len(chunknames)):
		filename = '../processed/jobs/align-%s.log'%(chunknames[index])        		
		print(filename)
		if os.path.exists(filename):
			line = os.popen("tail -n 1 %s" % filename).read()
			print line
			words = line.split()
			print words
			if words[0]=="After":
				print "True"
			else:
				return False
		else:
			return False
	return True


   def requires(self):

	print "3D alignment!"

	dirnames = glob.glob('../processed/*-*')
	chunknames = []
	
	for index in range (0, len(dirnames)):
		[left,sep,chunkname] = dirnames[index].rpartition('/')
		chunknames.append(chunkname)
		print chunkname
	#return checkfileexists("statetable")
	#return alignallchunks(chunks=chunknames, df=self.df) 
	return [alignchunk(chunknames[index]) for index in range (0,len(chunknames))]
	
   def run(self):
	print "Hello!"
		
	#mydir = os.getcwd() + '/../processed/jobs/'
	#for subdir, dirs, files in os.walk('../processed/jobs/'):
	#	for myfile in files:
        #		result = run_alignment.apply_async(args=[myfile,mydir], countdown=3)
			
