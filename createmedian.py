import luigi
import os
import shutil
import pandas as pd
from checkfileexists import checkfileexists
from focusmapping import focusmapping
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
from celery import Celery
from tasks import run_celerycommand
from RenderTarget import RenderTarget
import renderapi
from renderapi.transform import AffineModel
from renderapps.module.render_module import RenderModule,RenderParameters
from RenderTileParameters import RenderTileParameters
from RenderTileTarget import RenderTileTarget
from renderapi.tilespec import MipMapLevel
from renderapi.tilespec import ImagePyramid



class createmedian_section(luigi.Task):

   #ASSUMES THAT IT IS GETTING A STATE TABLE OF ONE SECTION FROM ONE RIBBON


   channelname = luigi.Parameter(default="Gephyrin")
   channelnum = luigi.IntParameter(default=1)
   section = luigi.IntParameter()
   df = luigi.Parameter()
   rootdir = luigi.Parameter()
   parameters = luigi.Parameter()
   tileId = luigi.Parameter()
   Z = 1
   directory = ""

   def output(self):
	#if (self.channelname[:4] == "DAPI"):
	#	medianfile = self.rootdir+"/processed/median/Median_DAPI.tif"
	#else:
	medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + "_Section_%04d_%s.tif"%(self.section,self.tileId)

	#return luigi.LocalTarget(medianfile)

	p = self.parameters
	p['input_stack'] = "Median_%s"%self.channelname
	return RenderTileTarget(p,self.tileId)

   def requires(self):
	df = self.df

	#use only ribbons on this particular nas/microscope
	df = df[df['full_path'].str.contains(self.rootdir)==True]

	#find the section of interest
	df = df[df['section']==self.section]


	df = df.sort_values('ribbon')
	earliestrib = df.iloc[0].loc['ribbon']

	sortsessiondf = df.sort_values('session')
	earliestsession = sortsessiondf.iloc[0].loc['session']

	print "This is the earliest ribbon! "
	print earliestrib

	if (self.channelname[:4] == "DAPI"):
		map_images = df[(df['ribbon']==earliestrib) & (df['ch']==self.channelnum) & (df['session']==earliestsession) ]
	else:
		map_images = df[(df['ribbon']==earliestrib) & (df['ch']==self.channelnum) ]

	row0=map_images.iloc[0]
	fullfname=row0.loc['full_path']
        [directory,sep,fname]=fullfname.rpartition('/');

	#calculate Z and choose input directory based on that
	zimages = df[(df['ribbon']==row0.loc['ribbon']) & (df['ch']==row0.loc['ch']) & (df['session']==row0.loc['session']) & (df['frame']==row0.loc['frame'])  & (df['section']==row0.loc['section'])]
	self.Z = len(zimages)
	if (self.Z > 10):
		if (self.channelname[:4] == "DAPI") :
			self.directory = "../processed/focusmappeddata/Ribbon%04d/Session0000/DAPI_%s/"%(earliestsession,earliestrib)
		else:
			self.directory = "../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/"%(earliestrib,row0.loc['session'],self.channelname)
	else:
		self.directory = directory

	#calculate input images
	F = []
	if (self.Z > 10):
		for index,row in map_images.iterrows():
			if (row.loc['zstack'] == 0):
				F.append(row)

		return [ focusmapping(df=df, row=F[index] ) for index in range (0,len(F)) ]
	else:
		for index,row in map_images.iterrows():
			F.append(row.loc['full_path'])
		return [ checkfileexists(filename=F[index]) for index in range (0,len(F)) ]

   def run(self):

	cmd = "python /pipeline/sharmi/allen_SB_code/at_code/make_median.py "
	cmd = cmd + " --inputDirectory " + self.directory
	cmd = cmd + " --filepart S%04d"%self.section
	print "MEDIAN COMMAND LINE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

	#medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + "_Section_%04d.tif"%self.section
	medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + "_Section_%04d_%s.tif"%(self.section,self.tileId)

	#if (self.channelname[:4] == "DAPI"):
	##		cmd = cmd + " --outputImage %s/processed/median/Median_DAPI.tif"%self.rootdir
	#	print cmd
	#else:
	cmd = cmd + " --outputImage %s"%medianfile
	print cmd

	fp = open('/pipeline/forcron/testsharmi_mediancmd','w')
	fp.write(cmd)
	fp.close()

	#run command
	os.system(cmd)
	#result = run_celerycommand.apply_async(args=[self.cmd,os.getcwd()])

	self.parameters['input_stack'] = "Acquisition_%s"%self.channelname
	mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
	#mod.run()
	ts = mod.render.run(renderapi.tilespec.get_tile_spec,mod.args['input_stack'], self.tileId)
	d = ts.to_dict()
	d['mipmapLevels'][0]['imageUrl'] = medianfile
	d['mipmapLevels'].pop(1)
	d['mipmapLevels'].pop(2)
	d['mipmapLevels'].pop(3)
	ts.from_dict(d)

	stackname = "Median_%s"%self.channelname
	self.parameters['input_stack'] = stackname
	mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
	#mod.run()
	mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=2, cycleStepNumber=1)
	mod.render.run(renderapi.client.import_tilespecs, mod.args['input_stack'], [ts])
	mod.render.run(renderapi.stack.set_stack_state,stackname,'COMPLETE')












class createmedian(luigi.Task):
   channelname = luigi.Parameter(default="Gephyrin")
   channelnum = luigi.IntParameter(default=1)
   df = luigi.Parameter()
   rootdir = luigi.Parameter()
   Z = 1
   directory = ""

   def output(self):
	if (self.channelname[:4] == "DAPI"):
		medianfile = self.rootdir+"/processed/median/Median_DAPI.tif"
	else:
		medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + ".tif"

	return luigi.LocalTarget(medianfile)

   def requires(self):
	#select image directory
	#df=pd.read_csv("statetable")
	df = self.df

	#use only ribbons on this particular nas/microscope
	df = df[df['full_path'].str.contains(self.rootdir)==True]


	df = df.sort_values('ribbon')
	earliestrib = df.iloc[0].loc['ribbon']

	sortsessiondf = df.sort_values('session')
	earliestsession = sortsessiondf.iloc[0].loc['session']

	print "This is the earliest ribbon! "
	print earliestrib

	if (self.channelname[:4] == "DAPI"):
		map_images = df[(df['ribbon']==earliestrib) & (df['ch']==self.channelnum) & (df['session']==earliestsession) ]
	else:
		map_images = df[(df['ribbon']==earliestrib) & (df['ch']==self.channelnum) ]

	row0=map_images.iloc[0]
	fullfname=row0.loc['full_path']
        [directory,sep,fname]=fullfname.rpartition('/');

	#calculate Z and choose input directory based on that
	zimages = df[(df['ribbon']==row0.loc['ribbon']) & (df['ch']==row0.loc['ch']) & (df['session']==row0.loc['session']) & (df['frame']==row0.loc['frame'])  & (df['section']==row0.loc['section'])]
	self.Z = len(zimages)
	if (self.Z > 10):
		if (self.channelname[:4] == "DAPI") :
			self.directory = "../processed/focusmappeddata/Ribbon%04d/Session0000/DAPI_%s/"%(earliestsession,earliestrib)
		else:
			self.directory = "../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/"%(earliestrib,row0.loc['session'],self.channelname)
	else:
		self.directory = directory

	#calculate input images
	F = []
	if (self.Z > 10):
		for index,row in map_images.iterrows():
			if (row.loc['zstack'] == 0):
				F.append(row)

		return [ focusmapping(df=df, row=F[index] ) for index in range (0,len(F)) ]
	else:
		for index,row in map_images.iterrows():
			F.append(row.loc['full_path'])
		return [ checkfileexists(filename=F[index]) for index in range (0,len(F)) ]

   def run(self):

	cmd = "python /pipeline/sharmi/allen_SB_code/at_code/make_median.py "
	cmd = cmd + " --inputDirectory " + self.directory
	print "MEDIAN COMMAND LINE!!"
	if (self.channelname[:4] == "DAPI"):
		cmd = cmd + " --outputImage %s/processed/median/Median_DAPI.tif"%self.rootdir
		print cmd
	else:
		cmd = cmd + " --outputImage %s/processed/median/Median_"%self.rootdir + self.channelname + ".tif"
		print cmd

	#run command
	os.system(cmd)
	#result = run_celerycommand.apply_async(args=[self.cmd,os.getcwd()])
