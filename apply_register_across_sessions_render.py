import luigi
import pandas as pd
from checkfileexists import checkfileexists
import os
#from extractfeatures import extractfeatures
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd())
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


def get_map_images(df,ribbon,session,section,channel,zstack):
        map_images = df[(df['ribbon']==ribbon) & (df['session']==session) & (df['section']==section) & (df['ch']==channel) & (df['zstack']==zstack) ]
        map_images=map_images.sort('frame')
        return map_images



def getrow(df,ribbon,session,section,channel,zstack):
        map_images = get_map_images(df,ribbon,session,section,channel,zstack)
        row=map_images.iloc[0]
        return row

def parse_from_row(row):
        res = {}
        res['fullfname']=row.loc['full_path']
        [dirs,sep,res['fname']]=res['fullfname'].rpartition('/');
        [res['channame'],sep1,right]=res['fname'].partition('_S');
        [res['rootdir'],sep,therest]=res['fullfname'].rpartition('raw');
        res['tileId']=row.loc['tileID']
        fullstring = res['fullfname'].replace('//','/')
        tok=fullstring.split("/")
        res['project'] =tok[3]
        return res

class applyregister(luigi.Task):
	ribbon = luigi.IntParameter()
	session = luigi.IntParameter()
	section = luigi.IntParameter()	
	channame = luigi.Parameter()
	chan = luigi.IntParameter()
	refsession = luigi.IntParameter()
	parameters = luigi.Parameter()
        df = luigi.Parameter()
	cmd = ""
	
	def output(self):
	#	if not os.path.exists("../processed/registration_transformspec/"):
	#		os.mkdir ("../processed/registration_transformspec/")
	#	if not os.path.exists("../processed/registration_tilepec/"):
	#		os.mkdir ("../processed/registration_tilespec/")

		#outputtilespec = "../processed/registration_tilespec/%s_%01d_rib%04dsess%04dsect%04d.json"%(self.channame,self.session,self.ribbon, self.session, self.section)				

		#return [luigi.LocalTarget(outputtilespec)]

                row = getrow(self.df,self.ribbon,self.session,self.section,self.chan,0)
                self.info = parse_from_row(row); info = self.info
                self.parameters['tileId'] = '%s'%info['tileId']
                self.parameters['render']['project'] = '%s'%info['project']
		
		self.parameters['input_stack'] = 'Registered_%s'%self.channame
                return RenderTarget(self.parameters)

	def requires(self):

		info = self.info

		#self.cmd = "java -cp /data/array_tomography/ForSharmi/sharmirender/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitc
		self.cmd = "java -cp /data/array_tomography/ForSharmi/sharmirender/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.ApplyRegistration "
		self.cmd = self.cmd + " --inputtilespec %s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.channame,self.ribbon,self.session,self.section)
		self.cmd = self.cmd + " --registeredtilespec %s/processed/registration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.session,self.ribbon,self.session,self.section)
		self.cmd = self.cmd + " --outputJson %s/processed/registration_tilespec/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.channame,self.ribbon,self.session,self.section)

		self.referencetransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.ribbon, self.refsession, self.section)
                self.outputtilespec = "%s/processed/registration_tilespec/%s_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.channame,self.ribbon, self.session, self.section)
                

		return []

	def run(self):
		print self.cmd
		os.system(self.cmd)
		self.parameters['input_stack'] = 'Registered_%s'%self.channame
                mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
                mod.run()
                mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=4, cycleStepNumber=1)
                mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [self.outputtilespec],1,self.referencetransformspec)


class applyregistersessions(luigi.Task):
	refsession = luigi.IntParameter()
        statetable = luigi.Parameter()
        owner = luigi.Parameter()

        parameters = {
                        "render":{
                        "host":"ibs-forrestc-ux1",
                        "port":8080,
                        "client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
                        }
        }


	#df =pd.read_csv("statetable")
	def output(self):
		self.parameters['render']['owner'] = self.owner
                self.df = pd.read_csv(self.statetable)
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
	def requires(self):
		self.parameters['render']['owner'] = self.owner
                self.df = pd.read_csv(self.statetable)
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
		R = []
		C = []
		S = []
		CHAN = []
		CH = []
		for (rib,sess,sect,ch) in uniq_sections:	
			map_images = self.df[(self.df['ribbon']==rib) & (self.df['session']==sess) & (self.df['section']==sect) & (self.df['ch']==ch) & (self.df['zstack']==0) ]
			map_images=map_images.sort('frame')
			row0=map_images.iloc[0]
        		fullfname=row0.loc['full_path']
        		[dirs,sep,fname]=fullfname.rpartition('/');
			[channame,sep1,right]=fname.partition('_S');
			#if (sess > self.refsession) | (sess < self.refsession):
			R.append(rib)
			C.append(sect)
			S.append(sess)
			CH.append(ch)
			CHAN.append(channame)
		return[applyregister(ribbon = R[i], section = C[i], session = S[i], channame = CHAN[i], chan = CH[i], refsession = self.refsession, parameters=self.parameters, df = self.df) for i in range (0, len(R))]		
	def run(self):
		print "running registration on all sections"


