import luigi
import sys
sys.path.insert(0,'/pipeline/sharmi/allen_SB_code/celery/')
sys.path.insert(0,'/usr/local/render-python-apps/renderapps/module/')
import os
from checkfileexists import checkfileexists
from checkrenderfileexists import checkrenderfileexists
from createmedian import createmedian
from focusmapping import focusmapping
from celery import Celery
from tasks import run_celerycommand
from render_module import RenderModule,RenderParameters
from RenderTileParameters import RenderTileParameters
import renderapi
from RenderTarget import RenderTarget
import json
from renderapi.utils import renderdump
from renderapi.tilespec import MipMapLevel
from renderapi.tilespec import ImagePyramid
from RenderTileTarget import RenderTileTarget

class flatfieldcorrection(luigi.Task):
   flatfieldcorrectedfile = luigi.Parameter()
   channelname=luigi.Parameter()
   channelnum= luigi.Parameter()
   inputfile=luigi.Parameter()
   df = luigi.Parameter()
   rootdir = luigi.Parameter()
   row = luigi.Parameter()
   parameters = luigi.Parameter()
   tileId = luigi.Parameter()

   def output(self):
	print "Printing file name: "
	print self.flatfieldcorrectedfile
	self.parameters['input_stack'] = "Flatfieldcorrected_%s"%self.channelname
	self.parameters['tileId'] = self.tileId
	#return luigi.LocalTarget(self.flatfieldcorrectedfile)
	return RenderTileTarget(self.parameters)

   def requires(self):
	if (self.channelname[:4] == "DAPI"):
		medianfile = self.rootdir+"/processed/median/Median_DAPI.tif"
	else:
		medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + ".tif"

	zimages = self.df[(self.df['ribbon']==self.row.loc['ribbon']) & (self.df['ch']==self.channelnum) & (self.df['session']==self.row.loc['session']) & (self.df['frame']==self.row.loc['frame'])  & (self.df['section']==self.row.loc['section'])]
	row0=zimages.iloc[0]
	#tileId=row0.loc['tileID']
	fullpath=row0.loc['full_path']
	tok=fullpath.split("/")
	project=tok[3]
	self.Z = len(zimages)

	self.parameters['input_stack'] = "Acquisition_%s"%self.channelname
	#self.parameters['tileId'] = tileId

	#if (self.Z > 10) :
	#	return [createmedian(channelname=self.channelname,channelnum=self.channelnum,df=self.df),focusmapping(df=self.df, row=self.row, rootdir=self.rootdir)]
	#else:
		#return [checkfileexists(self.inputfile), createmedian(channelname=self.channelname,channelnum=self.channelnum,df=self.df,rootdir=self.rootdir)]
	return [checkrenderfileexists(self.parameters), createmedian(channelname=self.channelname,channelnum=self.channelnum,df=self.df,rootdir=self.rootdir)]



   def run(self):
	print "flat field correction"
	self.cmd = "python /pipeline/sharmi/allen_SB_code/at_code/flatfield_correct.py "
	#if (self.Z > 10):
	#	focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(self.row['ribbon'],self.row['session'],self.channelname, self.channelname, self.row['section'], self.row['frame'])
	#	self.cmd = self.cmd + " --inputImage " + focusmappedfile
	#else:
	self.cmd = self.cmd + " --inputImage " + self.inputfile
	self.cmd = self.cmd + " --outputImage " + self.flatfieldcorrectedfile
	if (self.channelname[:4] == "DAPI"):
		self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_DAPI.tif"%self.rootdir
	else:
		self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_"%self.rootdir + self.channelname + ".tif"
	print self.cmd
	os.system(self.cmd)


	#get acquisition tilespec
	mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
	#mod.run()
	ts = mod.render.run(renderapi.tilespec.get_tile_spec,mod.args['input_stack'], self.tileId)

	ip = ImagePyramid()
	ip.update(MipMapLevel(0, imageUrl=self.flatfieldcorrectedfile))
	ts.ip = ip

	#change the stackname
	self.parameters['input_stack'] = "Flatfieldcorrected_%s"%self.channelname
	#upload to render
	mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
	#mod.run()
	mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'])
	mod.render.run(renderapi.client.import_tilespecs, mod.args['input_stack'], [ts])
