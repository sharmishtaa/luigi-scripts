import luigi
import pandas as pd
from flatfieldcorrection import flatfieldcorrection
import os
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
#sys.path.insert(0,'/nas/data/M236780_Scnn1aTdt_2/scripts/')
#sys.path.insert(0,'/nas/data/M239923_RorBTdt_2/scripts/')
sys.path.insert(0,os.getcwd())
		  
from celery import Celery
from tasks import run_celerycommand
from stitching import stitch_section
from checkfileexists import checkfileexists

class stitch_ribbon_session_channel(luigi.Task):
   #ribbon=3
   #channel=11
   #session=1
   #df=luigi.Parameter(default=pd.read_csv("statetable"))
   ribbon = luigi.IntParameter()
   channel = luigi.IntParameter()
   session = luigi.IntParameter()
   #df = luigi.Parameter()
   #df =pd.read_csv("statetable")
   statetablefile = luigi.Parameter()
   
   

   def output(self):
	self.df =pd.read_csv(self.statetablefile)
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['ch']==self.channel) & (self.df['session']==self.session) & (self.df['zstack']==0)]
        row0=map_images.iloc[0]
        fullfname=row0.loc['full_path']
        [dirs,sep,fname]=fullfname.rpartition('/');
	[channame,sep1,right]=fname.partition('_S');
	#F = []
	#for index, rows in map_images.iterrows():
	#	str = "../processed/stitched_files/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, rows.loc['section'], channame)
	#	F.append(str)

	
	#return [luigi.LocalTarget(F[index]) for index in range (0, len(F))]		

	F = []
	for index, rows in map_images.iterrows():
		jsonfilename = "../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,rows.loc['section'])
		F.append(jsonfilename)
		
	return [luigi.LocalTarget(F[index]) for index in range (0, len(F))]		

   def requires(self):
	self.df =pd.read_csv(self.statetablefile)
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['ch']==self.channel) & (self.df['session']==self.session) & (self.df['zstack']==0)]
	for index, row in map_images.iterrows():
		print index
		print self.ribbon
		print self.session
		print row.loc['section']
		print self.channel
	return [stitch_section(ribbon=self.ribbon, session=self.session, section=row.loc['section'], channel=self.channel, df=self.df) for index, row in map_images.iterrows() ]	

   def run(self):
	print "Running stitching on ribbon session channel!"
