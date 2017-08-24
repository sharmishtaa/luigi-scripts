import luigi
import json
import pandas as pd
#from flatfieldcorrection import flatfieldcorrection
import os
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
#sys.path.insert(0,os.getcwd())
sys.path.insert(0,'/usr/local/render-python-apps/renderapps/module/')
#from celery import Celery
#from tasks import run_celerycommand
from RenderTarget import RenderTarget
import renderapi
from renderapi.transform import AffineModel
from renderapps.module.render_module import RenderModule,RenderParameters
from pathos.multiprocessing import Pool
from functools import partial
import tempfile
import marshmallow as mm
import numpy as np
from RenderTileParameters import RenderTileParameters
from RenderTileTarget import RenderTileTarget
import glob
from renderapi.tilespec import MipMapLevel
from renderapi.tilespec import ImagePyramid
from createmedian import createmedian_section

def get_tileIds(df):
	tileIds = []
	for index,row in df.iterrows():
		tileIds.append(str(row.loc['tileID']))
	return tileIds



class acquisition_stacks(luigi.Task):
    	
	owner = luigi.Parameter()
	projectdirector = luigi.Parameter()
	projectname = luigi.Parameter()
	statetablefile = luigi.Parameter()
	
    def output(self):

		#get all tileIds from state table
		self.df=pd.read_csv(self.statetablefile)
		#check all tileIds
		return RenderTarget(self.parameters)	

    def requires(self):
	
		
		return [ff_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=info['channame'], channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=info['rootdir'], parameters = self.parameters, tileIds = self.tileIds, section =self.section)]
		
    def run(self):

		dcmd = "docker exec renderapps python append_fast_stacks.py "
		dcmd = dcmd + "--render.host ibs-forrestc-ux1 "
		dcmd = dcmd + "--render.client_scripts /var/www/render/render-ws-java-client/src/main/scripts "
		dcmd = dcmd + "--render.port 8080 "
		dcmd = dcmd + "--render.memGB 5G "
		dcmd = dcmd + "--log_level INFO "
		dcmd = dcmd + "--statetableFile %s "%self.statetablefile
		dcmd = dcmd + "--render.project %s "%self.projectname
		dcmd = dcmd + "--projectDirectory %s "%self.projectdirectory
		dcmd = dcmd + "--outputStackPrefix Acquisition "
		dcmd = dcmd + " --render.owner %s "%self.owner
		print dcmd
		os.system(dcmd)

        


