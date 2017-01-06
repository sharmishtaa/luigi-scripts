import luigi
import pandas as pd
from flatfieldcorrection import flatfieldcorrection
import os
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd())
from celery import Celery
from tasks import run_celerycommand
from stitching import stitch_section
from stitch_ribbon import stitch_ribbon_session_channel

class stitch_allchannel_sessions(luigi.Task):
   
   df=pd.read_csv("statetable")

   def output(self):
	uniq_channel_sessions = self.df.groupby(['ribbon','session','ch']).groups.keys()
   def requires(self):
	uniq_channel_sessions = self.df.groupby(['ribbon','session','ch']).groups.keys()
	R = []
	C = []
	S = []
	for (rib,sess,chan) in uniq_channel_sessions:	
		R.append(rib)
		C.append(chan)
		S.append(sess)
		 
	return [stitch_ribbon_session_channel(ribbon=R[index], session=S[index], channel=C[index], df=self.df) for index in range (0,len(R))]
	
   def run(self):
	print "Running stitching on full ribbon all sessions and all channels!"
