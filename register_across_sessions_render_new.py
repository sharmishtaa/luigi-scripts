import luigi import pandas as pd from checkfileexists import checkfileexists
import os import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd()) from RenderTarget import RenderTarget import
renderapi from renderapi.transform import AffineModel from
renderapps.module.render_module import RenderModule,RenderParameters from
pathos.multiprocessing import Pool from functools import partial import tempfile
import marshmallow as mm import numpy as np from RenderTileParameters import
RenderTileParameters from RenderTileTarget import RenderTileTarget import glob
from renderapi.tilespec import MipMapLevel from renderapi.tilespec import
ImagePyramid import json

#removing comments

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


class register(luigi.Task):
	ribbon = luigi.IntParameter()
	session = luigi.IntParameter()
	section = luigi.IntParameter()
	refsession = luigi.IntParameter()
	parameters = luigi.Parameter()
	df = luigi.Parameter()

	cmd = ""

	def output(self):


		#get row from df to parse
		channel = 0 #DAPI
		row = getrow(self.df,self.ribbon,self.session,self.section,channel,0)
		self.info = parse_from_row(row); info = self.info

		#adjust tile id for session num
		str_tileid = '%s'%info['tileId']
		pre_tileid = int(str_tileid[0]) - self.session
		new_tileid = str(pre_tileid) + str_tileid[1:]
		print new_tileid

		self.parameters['tileId'] = new_tileid
		self.parameters['render']['project'] = '%s'%info['project']



		#create outputdirectories if they do not exist
		if not os.path.exists("%s/processed/registration_tilespec/"%info['rootdir']):
			os.mkdir ("%s/processed/registration_tilespec/"%info['rootdir'])
		if not os.path.exists("%s/processed/registration_transformspec/"%info['rootdir']):
			os.mkdir ("%s/processed/registration_transformspec/"%info['rootdir'])

		outputtilespec = "%s/processed/registration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.session,self.ribbon, self.session, self.section)
		outputtransform = "%s/processed/registration_transformspec/rib%04dsess%04dsect%04d.json"%(info['rootdir'],self.ribbon, self.refsession, self.section)

		print "output tilespec and transform"
		print outputtilespec
		print outputtransform
		self.parameters['input_stack'] = 'Registered_DAPI%01d'%self.session
                #mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
                #mod.run()
                #mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=4, cycleStepNumber=1)
                #mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [outputtilespec], 1, outputtransform)

		#return luigi.LocalTarget(outputtilespec)
		return RenderTarget(self.parameters)
	def requires(self):


		zval = self.ribbon*100+self.section

		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		inputts = mod.render.run(renderapi.tilespec.get_tile_specs_from_z,"Stitched_DAPI_%d_dropped"%self.session,zval)
		referencets = mod.render.run(renderapi.tilespec.get_tile_specs_from_z,"Stitched_DAPI_%d_dropped"%self.refsession,zval)
		#create outputdirectories if they do not exist
		if not os.path.exists("%s/processed/stitched_tilespec_ff_dropped/"%self.info['rootdir']):
			os.mkdir ("%s/processed/stitched_tilespec_ff_dropped/"%self.info['rootdir'])
		if not os.path.exists("%s/processed/stitched_tilespec_ff_dropped/"%self.info['rootdir']):
			os.mkdir ("%s/processed/stitched_tilespec_ff_dropped/"%self.info['rootdir'])

		inputtilespec = "%s/processed/stitched_tilespec_ff_dropped/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.session,self.ribbon, self.session, self.section)
		referencetilespec = "%s/processed/stitched_tilespec_ff_dropped/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.refsession,self.ribbon, self.refsession, self.section)

		#mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		#mod.run()
		#mod.render.run(renderapi.utils.renderdump,inputts, inputtilespec)
		#mod.render.run(renderapi.utils.renderdump,referencets, referencetilespec)
		inputfile = open(inputtilespec,"w")
		referencefile = open(referencetilespec,"w")
		renderapi.utils.renderdump(inputts,inputfile)
		renderapi.utils.renderdump(referencets,referencefile)



		inputtransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.ribbon, self.session, self.section)
		referencetransformspec = "%s/processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.ribbon, self.refsession, self.section)
		outputtilespec = "%s/processed/registration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.session,self.ribbon, self.session, self.section)
		outputtransform = "%s/processed/registration_transformspec/rib%04dsess%04dsect%04d.json"%(self.info['rootdir'],self.ribbon, self.session, self.section)
		refID = "rib%04dsess%04dsect%04d_to_session%01d"%(self.ribbon, self.session, self.section,self.refsession)

		#self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.RegisterSections --modelType 1 --percentSaturated 0.9f --maxEpsilon 2.5 --initialSigma 1.6 --inputtilespec "
		#self.cmd = "java -cp /pipeline/stitching/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.RegisterSections --modelType 1 --percentSaturated 0.9f --maxEpsilon 2.5 --initialSigma 2.5 --inputtilespec "
		#self.cmd = "java -cp /data/array_tomography/ForSharmi/sharmirender/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.RegisterSections --modelType 1 --percentSaturated 0.9f --maxEpsilon 20.0f--initialSigma 1.6f  --maxOctaveSize 2048 --minOctaveSize 200 --inputtilespec "
		self.cmd = "java -cp /pipeline/sharmi/sharmirender/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/pipeline/stitching/Fiji_Plugins-2.0.1.jar org.janelia.alignment.RegisterSections --modelType 1 --percentSaturated 0.9f --maxEpsilon 2.5 --initialSigma 2.5 --inputtilespec "
		self.cmd = self.cmd + inputtilespec + " --inputtransformspec " + inputtransformspec + " --referencetilespec " + referencetilespec + " --referencetransformspec " + referencetransformspec +  " --outputJson " + outputtransform + " --outputtilespec " + outputtilespec + " --referenceID " + refID
		self.outputtilespec = outputtilespec
		self.outputtransform = outputtransform
		self.referencetransformspec = referencetransformspec

		return []
	def run(self):
		print self.cmd
		os.system(self.cmd)
		print "THIS IS OUTPUTTILESPEC!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

		print self.parameters['input_stack']
		#upload to render
		self.parameters['input_stack'] = 'Registered_DAPI%01d'%self.session

		#self.outputtilespec = "/nas2//data/HumanIHCopt_rnd1_control//processed/registration_tilespec/DAPI_3_rib0029sess0003sect0005.json"
		#self.referencetransformspec = "/nas2//data/HumanIHCopt_rnd1_control//processed/stitched_transformspec_ff/rib0029sess0001sect0005.json"



		print self.outputtilespec
		print self.referencetransformspec

		print "STARTING UPLOADDDDDDDDDDDDD"

		mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
		mod.run()
		mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=4, cycleStepNumber=1)
		mod.render.run(renderapi.client.import_jsonfiles_parallel,self.parameters['input_stack'], [self.outputtilespec],1,self.referencetransformspec)


        	print "UPLOADEDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"
        	#mod.render.run(renderapi.stack.create_stack,mod.args['input_stack'],cycleNumber=4, cycleStepNumber=1)
        	#mod.render.run(renderapi.client.import_jsonfiles, mod.args['input_stack'], [self.outputtilespec])
        	#mod.render.run(renderapi.stack.set_stack_state,mod.args['input_stack'],'COMPLETE')

class registersessions(luigi.Task):
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
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
	def requires(self):
		self.parameters['render']['owner'] = self.owner
		self.df = pd.read_csv(self.statetable)
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
		R = []
		C = []
		S = []
		for (rib,sess,sect) in uniq_sections:
			#if (sess == 2) & (rib == 1) & (sect == 0):
			#if (sess > self.refsession) | (sess < self.refsession):
			if (sess>0):
				R.append(rib)
				C.append(sect)
				S.append(sess)
		return[register(ribbon = R[i], section = C[i], session = S[i], refsession = self.refsession, parameters=self.parameters, df = self.df) for i in range (0, len(R))]
	def run(self):
		print "running registration on all sections"
