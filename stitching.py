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


class stitch_dapi_section(luigi.Task):
    ribbon = luigi.Parameter()
    section = luigi.Parameter()
    session = luigi.Parameter()
    channel = 0
    df = luigi.Parameter()

    def output(self):
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ]
	map_images=map_images.sort('frame')
	row0=map_images.iloc[0]
        fullfname=row0.loc['full_path']
        [dirs,sep,fname]=fullfname.rpartition('/');
	[channame,sep1,right]=fname.partition('_S');
	
	imagefilename = "../processed/stitched_files_json_ff/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, self.section, channame)
	jsonfilename = "../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	transformfilename = "../processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(self.ribbon,self.session,self.section)
	#return [luigi.LocalTarget(jsonfilename), luigi.LocalTarget(imagefilename)]
	if not os.path.exists("../processed/stitched_tilespec_ff/"):
		os.mkdir ("../processed/stitched_tilespec_ff/")
	if not os.path.exists("../processed/stitched_transformspec_ff/"):
		os.mkdir ("../processed/stitched_transformspec_ff/")
	return [luigi.LocalTarget(jsonfilename), luigi.LocalTarget(transformfilename)]

    def requires(self):
	
	#global variables
	self.F = []
	self.I = []
	self.x = []
	self.y = []
	self.allrows=[]

	#setup command

	#populate command and list dependencies
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ]
	map_images=map_images.sort('frame')
	row0=map_images.iloc[0]
        fullfname=row0.loc['full_path']
        [dirs,sep,fname]=fullfname.rpartition('/');
	[channame,sep1,right]=fname.partition('_S');
	[rootdir,sep,therest]=fullfname.rpartition('raw');


	self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.StitchImagesByCC "
	self.cmd = self.cmd + " --inputtilespec ../processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	self.cmd = self.cmd + " --outputtilespec ../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	self.cmd = self.cmd + " --outputtransformspec ../processed/stitched_transformspec_ff/rib%04dsess%04dsect%04d.json"%(self.ribbon,self.session,self.section)
	self.cmd = self.cmd + " --outputImage ../processed/stitched_files_json_ff/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, self.section, channame)
	#self.cmd = self.cmd + " --outputImage  ../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	for index,row in map_images.iterrows():
		fullpath = row.loc['full_path']
		[projectpath,datapath] = fullpath.split('raw')
		print projectpath
		#processedpath = projectpath+"processed/"
		processedpath = "../processed/"
	        thisframe =row.loc['frame']
		str = '%sflatfieldcorrecteddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(processedpath,self.ribbon,self.session,channame, channame, self.section,thisframe)
		str1 = str + ","
		self.F.append(str)
		self.I.append(row.loc['full_path'])
		self.allrows.append(row)
		#self.cmd = self.cmd  + str1

      	#dependencies
	return [  flatfieldcorrection(flatfieldcorrectedfile=self.F[index], channelname=channame, channelnum=self.channel, inputfile=self.I[index], df=self.df, row=self.allrows[index], rootdir=rootdir) for index in range (0,len(self.F)) ]

    def run(self):

	print "RUNNING STITCHING of one section!"
	print self.cmd
	os.system(self.cmd)
	#result = run_celerycommand.apply_async(args=[self.cmd,os.getcwd()])




class flatfieldcorrect_section(luigi.Task):
    flatfieldfilenames = luigi.Parameter()
    inputfilenames = luigi.Parameter()
    channelname = luigi.Parameter()
    channelnum = luigi.Parameter()
    allrows = luigi.Parameter()
    df = luigi.Parameter()
    rootdir = luigi.Parameter()

    def output(self):
	return [ luigi.LocalTarget(self.flatfieldfilenames[index]) for index in range (0,len(self.flatfieldfilenames)) ]
    def requires(self):
	return [ flatfieldcorrection(flatfieldcorrectedfile=self.flatfieldfilenames[index], channelname=self.channelname, channelnum=self.channelnum, inputfile=self.inputfilenames[index], df=self.df, row=self.allrows[index],rootdir=self.rootdir) for index in range (0,len(self.flatfieldfilenames)) ]

    def run(self):
	print "Running flat field correction on all files in section"



class stitch_section(luigi.Task):
    ribbon = luigi.IntParameter()
    section = luigi.IntParameter()
    session = luigi.IntParameter()
    channel = luigi.IntParameter()
    #df=pd.read_csv("statetable_0112")
    #df = luigi.Parameter(default=pd.read_csv("statetable"))
    df = luigi.Parameter()

    def output(self):
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ]
	#map_images = self.df[(self.df['ribbon']==0) & (self.df['session']==0) & (self.df['section']==0) & (self.df['ch']==0) & (self.df['zstack']==0) ]
	#map_images = self.df[(self.df['ribbon']==0) & (self.df['session']==0) & (self.df['ch']==0) & (self.df['zstack']==0) & (self.df['section']==9)]

	map_images=map_images.sort('frame')
	row0=map_images.iloc[0]
        fullfname=row0.loc['full_path']
        [dirs,sep,fname]=fullfname.rpartition('/');
	[channame,sep1,right]=fname.partition('_S');
	

	imagefilename = "../processed/stitched_files_json_ff/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, self.section, channame)
	if not os.path.exists("../processed/stitched_tilespec_ff/"):
		os.mkdir ("../processed/stitched_tilespec_ff/")
	if not os.path.exists("../processed/stitched_transformspec_ff/"):
		os.mkdir ("../processed/stitched_transformspec_ff/")
	jsonfilename = "../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	return [luigi.LocalTarget(jsonfilename)]

    def requires(self):
	
	#global variables
	self.F = []
	self.I = []
	self.x = []
	self.y = []
	self.allrows=[]

	#populate command and list dependencies
	map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ]
	map_images=map_images.sort('frame')
	row0=map_images.iloc[0]
        fullfname=row0.loc['full_path']
        [dirs,sep,fname]=fullfname.rpartition('/');
	[channame,sep1,right]=fname.partition('_S');
	[rootdir,sep,therest]=fullfname.rpartition('raw');

	self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.ApplyStitching "
	self.cmd = self.cmd + " --inputtilespec ../processed/flatfieldcorrected_tilespec/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)
	self.cmd = self.cmd + " --stitchedtilespec ../processed/stitched_tilespec_ff/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.session,self.ribbon,self.session,self.section)
	self.cmd = self.cmd + " --outputtilespec ../processed/stitched_tilespec_ff/%s_rib%04dsess%04dsect%04d.json"%(channame,self.ribbon,self.session,self.section)


	for index,row in map_images.iterrows():
	        thisframe =row.loc['frame']
		str = '%sflatfieldcorrecteddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%("../processed/",self.ribbon,self.session,channame, channame, self.section,thisframe)
		str1 = str + ","
		self.F.append(str)
		self.I.append(row.loc['full_path'])
		self.allrows.append(row)
		#self.cmd = self.cmd  + str1

	
	return [flatfieldcorrect_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=channame, channelnum=self.channel, allrows=self.allrows, df=self.df,rootdir=rootdir),  stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=row0.loc['section'],  df=self.df)]
	#return [ stitch_dapi_section(ribbon=self.ribbon, session=self.session, section=row0.loc['section'],  df=self.df)]

    def run(self):

	print "RUNNING APPLYING STITCHING of one section!"
	print self.cmd
	os.system(self.cmd)
	#result = run_celerycommand.apply_async(args=[self.cmd,os.getcwd()])

