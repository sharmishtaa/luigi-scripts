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

def create_stitch_command (info, ribbon, session, section):
	#cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.StitchImagesByCC "
	cmd = "java -cp /pipeline/stitching/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.StitchImagesByCC "
	cmd = cmd + " --inputtilespec %s/processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],ribbon,session,section)
	cmd = cmd + " --outputtilespec %s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],ribbon,session,section)
	cmd = cmd + " --outputtransformspec %s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(info['rootdir'],ribbon,session,section)
	cmd = cmd + " --outputImage %s/processed/stitched_files_json_ff/rib%04dsess%04dsect%04d_%s.tif"%(info['rootdir'],ribbon, session, section, info['channame'])
	return cmd	
	
def create_applystitch_command(info,ribbon,session,section):
	#cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.ApplyStitching "
	cmd = "java -cp /pipeline/stitching/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.ApplyStitching "
	cmd = cmd + " --inputtilespec %s/processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],ribbon,session,section)
	cmd = cmd + " --stitchedtilespec %s/processed/stitched_tilespec_ff/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(info['rootdir'],session,ribbon,session,section)
	cmd = cmd + " --outputtilespec %s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],ribbon,session,section)
	return cmd


def populate_dependencies(df,ribbon,session,section,channel,zstack,info):
	F = []; I = []	; allrows=[]
	map_images=get_map_images(df,ribbon,session,section,channel,zstack)
	for index,row in map_images.iterrows():
			fullpath = row.loc['full_path']
			[projectpath,datapath] = fullpath.split('raw')
			processedpath = "%sprocessed/"%info['rootdir']
			thisframe =row.loc['frame']
			str = '%sflatfieldcorrecteddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(processedpath,ribbon,session,info['channame'], info['channame'], section,thisframe)
			str1 = str + ","
			F.append(str)
			I.append(row.loc['full_path'])
			allrows.append(row)
	return [F,I,allrows]

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
		
def get_tileIds(df, ribbon, session, section, channel, zstack):
	tileIds = []
	map_images = get_map_images(df, ribbon, session, section, channel, zstack)
	for index,row in map_images.iterrows():
	#for mi in map_images:
		tileIds.append(str(row.loc['tileID']))
	return tileIds

def generate_ff_imagename(inputfilename):
	orig = "raw/data"
	ff = "processed/flatfieldcorrecteddata"
	fname = inputfilename.replace(orig,ff)
	
	origsess = "session"
	ffsess = "Session00"
	fname = fname.replace(origsess,ffsess)
	return fname

class stitch_dapi_section(luigi.Task):
    ribbon = luigi.Parameter()
    section = luigi.Parameter()
    session = luigi.Parameter()
    df = luigi.Parameter()
    parameters = luigi.Parameter()
    #ribbon = 112
    #section = 0
    #session = 1
    channel = 0
    #df=pd.read_csv("/nas3/data/M270907_Scnn1aTg2Tdt_13/scripts_ff/statetable_0112")
    
	
    def output(self):

		#get row from df to parse
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		self.info = parse_from_row(row); info = self.info
		
		#extract all information
		self.parameters['tileId'] = '%s'%info['tileId']
		self.parameters['render']['project'] = '%s'%info['project']
		self.parameters['session'] = self.session
		
		#create outputdirectories if they do not exist
		if not os.path.exists("%s/processed/stitched_tilespec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_tilespec_ff/"%info['rootdir'])
		if not os.path.exists("%s/processed/stitched_transformspec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_transformspec_ff/"%info['rootdir'])
		if not os.path.exists("%s/processed/flatfieldcorrected_tilespec/"%info['rootdir']):
			os.mkdir ("%s/processed/flatfieldcorrected_tilespec/"%info['rootdir'])	

		#return target
		self.parameters['input_stack'] = 'Stitched_%s'%info['channame']
		return RenderTarget(self.parameters)	

    def requires(self):
	
		#global variables
		self.F = [];self.I = []	; self.allrows=[]

		#setup command
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		self.info = parse_from_row(row); info = self.info
		info = self.info #should be set by the output method
		
		#outputfiles
		self.outputtilespec = "%s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],self.ribbon,self.session,self.section)
		self.outputtransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.ribbon,self.session,self.section)
		
		#populate command and list dependencies
		self.cmd = create_stitch_command (self.info, self.ribbon, self.session, self.section)
		[self.F,self.I,self.allrows] = populate_dependencies(self.df,self.ribbon,self.session,self.section,self.channel,0,info)
		self.tileIds = get_tileIds(self.df,self.ribbon,self.session,self.section,self.channel,0)
		
		return [ff_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=info['channame'], channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=info['rootdir'], parameters = self.parameters, tileIds = self.tileIds, section =self.section)]
		
    def run(self):

		stackname = 'Flatfieldcorrected_%s'%self.info['channame']
		self.parameters['input_stack'] = 'Flatfieldcorrected_%s'%self.info['channame']
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.set_stack_state,stackname,'COMPLETE')
		
		#get z and save ff tilespec - temporary!
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		z = row.loc['z']
		tilespeclist = mod.render.run(renderapi.tilespec.get_tile_specs_from_z,mod.args['input_stack'], z)
		json_text=json.dumps([t.to_dict() for t in tilespeclist],indent=4)
		json_file = "%s/processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.info['channame'],self.ribbon,self.session,self.section)
		fd=open(json_file, "w")
		fd.write(json_text)
		fd.close()


		print "RUNNING STITCHING of one section!"
		print self.cmd
		os.system(self.cmd)
				
		
		self.parameters['input_stack'] = 'Stitched_%s'%self.info['channame']
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=3, cycleStepNumber=1)
		mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [self.outputtilespec], 1, self.outputtransformspec)

        


class flatfieldcorrect_section(luigi.Task):
	flatfieldfilenames = luigi.Parameter()
	inputfilenames = luigi.Parameter()
	channelname = luigi.Parameter()
	channelnum = luigi.Parameter()
	allrows = luigi.Parameter()
	df = luigi.Parameter()
	rootdir = luigi.Parameter()
	parameters = luigi.Parameter()
	tileIds = luigi.Parameter()
	
	def output(self):
		self.allparams = []
		for t in self.tileIds:
			p = self.parameters
			
			p['input_stack'] = "Flatfieldcorrected_%s"%self.channelname
			p['tileId'] = t
			self.allparams.append(p)
		#print self.flatfieldfilenames[0]
		#return luigi.LocalTarget(self.flatfieldfilenames[0])
		#print self.flatfieldfilenames
		#return RenderTileTarget (p,tileId=self.tileIds[40])
		return [ RenderTileTarget(self.allparams[index],tileId=self.tileIds[index]) for index in range (0,len(self.allparams)) ]
		#return [ RenderTarget(self.allparams[index]) for index in range (0,len(self.allparams)) ]
		#return [ luigi.LocalTarget(self.flatfieldfilenames[index]) for index in range (0,len(self.flatfieldfilenames)) ]
	def requires(self):
		index = 40
		#print self.allparams[0]
		#return flatfieldcorrection(flatfieldcorrectedfile=self.flatfieldfilenames[index], channelname=self.channelname, channelnum=self.channelnum, inputfile=self.inputfilenames[index], df=self.df, row=self.allrows[index],rootdir=self.rootdir, parameters = self.allparams[index],tileId = self.tileIds[index])
		return [ flatfieldcorrection(flatfieldcorrectedfile=self.flatfieldfilenames[index], channelname=self.channelname, channelnum=self.channelnum, inputfile=self.inputfilenames[index], df=self.df, row=self.allrows[index],rootdir=self.rootdir, parameters = self.allparams[index], tileId = self.tileIds[index]) for index in range (0,len(self.flatfieldfilenames)) ]
		
	def run(self):
		print "Running flat field correction on all files in section"
		#just to complete the stack.
		stackname = "Flatfieldcorrected_%s"%self.channelname
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.set_stack_state,stackname,'COMPLETE')
		
		
		
class ff_section(luigi.Task):
	flatfieldfilenames = luigi.Parameter()
	inputfilenames = luigi.Parameter()
	channelname = luigi.Parameter()
	channelnum = luigi.Parameter()
	allrows = luigi.Parameter()
	df = luigi.Parameter()
	rootdir = luigi.Parameter()
	parameters = luigi.Parameter()
	tileIds = luigi.Parameter()
	section = luigi.Parameter()
	
	def output(self):
		self.allparams = []
		for t in self.tileIds:
			p = self.parameters
			
			p['input_stack'] = "Flatfieldcorrected_%s"%self.channelname
			p['tileId'] = t
			self.allparams.append(p)
		return [ RenderTileTarget(self.allparams[index],tileId=self.tileIds[index]) for index in range (0,len(self.allparams)) ]
	def requires(self):
		return [createmedian_section(channelname=self.channelname,channelnum=self.channelnum,df=self.df,rootdir=self.rootdir, section =self.section, parameters=self.parameters, tileId = self.tileIds[0])]
	def run(self):
		allts = []
		for i in range(0,len(self.tileIds)):
			#get acquisition tilespec
			
			self.parameters['input_stack'] = "Acquisition_%s"%self.channelname
			mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
			mod.run()
			ts = mod.render.run(renderapi.tilespec.get_tile_spec,mod.args['input_stack'], self.tileIds[i])
			#create command
			#inputfilename = ts.imageUrl
			d = ts.to_dict()
			print d['tileId']
			inputfilename = d['mipmapLevels'][0]['imageUrl']
			flatfieldfilename = generate_ff_imagename(inputfilename)
			#medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + "_Section_%04d.tif"%self.section
			medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + "_Section_%04d_%s.tif"%(self.section,self.tileIds[0])
			self.cmd = "python /data/array_tomography/ForSharmi/allen_SB_code/MakeAT/flatfield_correct.py "
			self.cmd = self.cmd + " --inputImage " + inputfilename
			self.cmd = self.cmd + " --outputImage " + flatfieldfilename
			self.cmd = self.cmd + " --flatfieldStandardImage %s"%medianfile
			
			#if (self.channelname[:4] == "DAPI"):
			#	self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_DAPI.tif"%self.rootdir
			#else:
			#	self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_"%self.rootdir + self.channelname + ".tif"
			
			#ip = ImagePyramid()
			#ip.update(MipMapLevel(0, imageUrl=flatfieldfilename))
			d['mipmapLevels'][0]['imageUrl'] = flatfieldfilename
			d['mipmapLevels'].pop(1)
			d['mipmapLevels'].pop(2)
			d['mipmapLevels'].pop(3)
			ts.from_dict(d) 
			
			allts.append(ts)
			print self.cmd	
			#run command
			os.system(self.cmd)
			
		#upload
		
		self.parameters['input_stack'] = "Flatfieldcorrected_%s"%self.channelname
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=2, cycleStepNumber=1)
		mod.render.run(renderapi.client.import_tilespecs, mod.args['input_stack'], allts)
			


class stitch_section(luigi.Task):
	
    ribbon = luigi.IntParameter()
    section = luigi.IntParameter()
    session = luigi.IntParameter()
    statetablefile = luigi.Parameter()
    channel = luigi.IntParameter()
    owner = luigi.Parameter()
    
    #df = luigi.Parameter()
    
    #ribbon = 112
    #section = 0
    #session = 1
    #channel = 1
    #df=pd.read_csv("/nas3/data/M270907_Scnn1aTg2Tdt_13/scripts_ff/statetable_0112")
    parameters = {
			"render":{
			"host":"ibs-forrestc-ux1",
			"port":8080,
			"client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
			}
	}
	
    def output(self):
		self.parameters['render']['owner'] = self.owner
		
		self.df=pd.read_csv(self.statetablefile)
		
		#get row from df to parse
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		self.info = parse_from_row(row); info = self.info
		
		#extract all information
		self.parameters['tileId'] = '%s'%info['tileId']
		self.parameters['render']['project'] = '%s'%info['project']
		self.parameters['session'] = self.session
		
		#create outputdirectories if they do not exist
		if not os.path.exists("%s/processed/stitched_tilespec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_tilespec_ff/"%info['rootdir'])
		if not os.path.exists("%s/processed/stitched_transformspec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_transformspec_ff/"%info['rootdir'])
		if not os.path.exists("%s/processed/flatfieldcorrected_tilespec/"%info['rootdir']):
			os.mkdir ("%s/processed/flatfieldcorrected_tilespec/"%info['rootdir'])	

		#return target
		
		self.parameters['input_stack'] = 'Stitched_%s'%info['channame']
		return RenderTarget(self.parameters)	

    def requires(self):
	
		self.parameters['render']['owner'] = self.owner
		
		#global variables
		self.F = [];self.I = []	; self.allrows=[]

		#setup command
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		self.info = parse_from_row(row); info = self.info
		info = self.info #should be set by the output method
		
		#outputfiles
		self.outputtilespec = "%s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],self.ribbon,self.session,self.section)
		self.outputtransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.ribbon,self.session,self.section)
		
		#populate command and list dependencies
		self.cmd = create_applystitch_command (self.info, self.ribbon, self.session, self.section)
		[self.F,self.I,self.allrows] = populate_dependencies(self.df,self.ribbon,self.session,self.section,self.channel,0,info)
		self.tileIds = get_tileIds(self.df,self.ribbon,self.session,self.section,self.channel,0)
		return [ff_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=info['channame'], channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=info['rootdir'], parameters = self.parameters, tileIds = self.tileIds, section=self.section),stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=self.section,  df=self.df, parameters=self.parameters)]
		#return [ff_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=info['channame'], channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=info['rootdir'], parameters = self.parameters, tileIds = self.tileIds, section=self.section)]
		
    def run(self):
		self.parameters['render']['owner'] = self.owner

		stackname = 'Flatfieldcorrected_%s'%self.info['channame']
		
		self.parameters['input_stack'] = 'Flatfieldcorrected_%s'%self.info['channame']
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.set_stack_state,stackname,'COMPLETE')

		#get z and save ff tilespec - temporary!
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		z = row.loc['z']
		tilespeclist = mod.render.run(renderapi.tilespec.get_tile_specs_from_z,mod.args['input_stack'], z)
			

		json_text=json.dumps([t.to_dict() for t in tilespeclist],indent=4)
		json_file = "%s/processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.info['channame'],self.ribbon,self.session,self.section)
		fd=open(json_file, "w")
		fd.write(json_text)
		fd.close()


		print "RUNNING APPLYING STITCHING of one section!"
		print self.cmd
		os.system(self.cmd)
		
		
		self.parameters['input_stack'] = 'Stitched_%s'%self.info['channame']
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=3, cycleStepNumber=1)
		mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [self.outputtilespec], 1, self.outputtransformspec)





			
class stitch_section_old(luigi.Task):
	#ribbon = luigi.IntParameter()
	#section = luigi.IntParameter()
	#session = luigi.IntParameter()
	#channel = luigi.IntParameter()
	#owner = luigi.Parameter()
	#df = luigi.Parameter()
	ribbon = 112; section = 1; session = 1; channel = 1; df=pd.read_csv("/nas3/data/M270907_Scnn1aTg2Tdt_13/scripts_ff/statetable_0112"); owner = "Sharmishtaas"
	
	parameters = {
			"render":{
			"host":"ibs-forrestc-ux1",
			"port":8080,
			"owner":"Sharmishtaas",
			"client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
			}
	}
	

	def output(self):
		
		#get row from df to parse
		row = getrow(self.df,self.ribbon,self.session,self.section,self.channel,0)
		self.info = parse_from_row(row); info = self.info
		
		#extract all information
		self.parameters['tileId'] = '%s'%info['tileId']
		self.parameters['render']['project'] = '%s'%info['project']
		self.parameters['input_stack'] = 'STITCHEDSTACKFINAL_MARCH31_%s'%info['channame']
		self.parameters['owner'] = self.owner

		if not os.path.exists("%s/processed/stitched_tilespec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_tilespec_ff/"%info['rootdir'])
		if not os.path.exists("%s/processed/stitched_transformspec_ff/"%info['rootdir']):
			os.mkdir ("%s/processed/stitched_transformspec_ff/"%info['rootdir'])
		print self.parameters
		
		return RenderTarget(self.parameters)

	def requires(self):
		
		#global variables
		self.F = [];self.I = []	; self.allrows=[]

		#setup command
		info = self.info #should be set by the output method
		
		#outputfiles
		self.outputtilespec = "%s/processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(info['rootdir'],info['channame'],self.ribbon,self.session,self.section)
		self.outputtransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.ribbon,self.session,self.section)
		
		#populate command and list dependencies
		self.cmd = create_applystitch_command (self.info, self.ribbon, self.session, self.section)
		[self.F,self.I,self.allrows] = populate_dependencies(self.df,self.ribbon,self.session,self.section,self.channel,0,info)
		self.tileIds = get_tileIds(self.df,self.ribbon,self.session,self.section,self.channel,0)
		
		return [flatfieldcorrect_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=info['channame'], channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=info['rootdir'], parameters = self.parameters, tileIds = self.tileIds),  stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=self.section,  df=self.df)]
		#return [ stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=row0.loc['section'],  df=self.df)]
		#return [flatfieldcorrect_section(tileIds=self.tileIds, parameters = self.parameters),  stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=self.section,  df=self.df)]
		
	def run(self):

		print "RUNNING APPLYING STITCHING of one section!"
		print self.cmd
		os.system(self.cmd)
		self.parameters['input_stack'] = 'STITCHEDSTACKFINAL_MARCH31_%s'%info['channame']
		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'])
		mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [self.outputtilespec], 1, self.outputtransformspec)


