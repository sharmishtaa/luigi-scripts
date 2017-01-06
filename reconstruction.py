import luigi
import pandas as pd
from checkfileexists import checkfileexists
from  flatfieldcorrection import flatfieldcorrect_section
import os
from extractfeatures import extractfeatures
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd())

class reconstruction(luigi.Task):
	ribbon = luigi.IntParameter()
	session = luigi.IntParameter()
	section = luigi.IntParameter()	
	channel = luigi.IntParameter()
	df=luigi.Parameter()
	cmd = ""

	def output(self):
		map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ] # z stack 0 ensures you get only one image per stack
		map_images=map_images.sort('frame')
		row0=map_images.iloc[0]
        	fullfname=row0.loc['full_path']
	        [dirs,sep,fname]=fullfname.rpartition('/');
		[channame,sep1,right]=fname.partition('_S');
		channame1 = channame
		if channame[:4]=="DAPI":
			channame1="DAPI"
		outputfile = "../processed/reconstruction/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, self.section, channame1)
		return luigi.LocalTarget(outputfile)	
	def requires(self):

		self.F = []
		self.I = []
		self.x = []
		self.y = []
		self.allrows=[]
		

		map_images = self.df[(self.df['ribbon']==self.ribbon) & (self.df['session']==self.session) & (self.df['section']==self.section) & (self.df['ch']==self.channel) & (self.df['zstack']==0) ] # z stack 0 ensures you get only one image per stack
		map_images=map_images.sort('frame')
		row0=map_images.iloc[0]
        	fullfname=row0.loc['full_path']
	        [dirs,sep,fname]=fullfname.rpartition('/');
		[channame,sep1,right]=fname.partition('_S');
		channame1 = channame
		if channame[:4]=="DAPI":
			channame1="DAPI"

		layoutfile = "../processed/stitch_layouts/rib%04dsess%04dsect%04d_stitch.txt"%(self.ribbon, self.session, self.section)
		registrationtransformfile = "../processed/registration_transforms/rib%04dsess%04dsect%04d.txt"%(self.ribbon, self.session, self.section)
		alignmenttransformfile = "../processed/export/rib%04dsess0001sect%04d_DAPI.tif.xml"%(self.ribbon, self.section)
		outputfile = "../processed/reconstruction/rib%04dsess%04dsect%04d_%s.tif"%(self.ribbon, self.session, self.section, channame1)

		self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/newForrestFijiBento/target/render-0.0.1-SNAPSHOT.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar  org.janelia.alignment.ReconstructImage "
		self.cmd = self.cmd + " --boundBoxFile ../processed/export/boundbox.xml "
		self.cmd = self.cmd + " --imageFiles "

		for index,row in map_images.iterrows():
	        	thisframe =row.loc['frame']
			str = '%sflatfieldcorrecteddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%("../processed/",self.ribbon,self.session,channame, channame, self.section,thisframe)
			str1 = str + ","
			self.F.append(str)
			self.I.append(row.loc['full_path'])
			self.allrows.append(row)
			self.cmd = self.cmd  + str1

		self.cmd = self.cmd + " --layoutFile " + layoutfile
		if (self.session > 1):
			self.cmd = self.cmd + " --registrationTransformFile " + registrationtransformfile
		self.cmd = self.cmd + " --alignmentTransformFile " + alignmenttransformfile
		self.cmd = self.cmd + " --outputFile " + outputfile
		return [ flatfieldcorrect_section(flatfieldfilenames=self.F, inputfilenames=self.I, channelname=channame, channelnum=self.channel, allrows=self.allrows, df=self.df) ]

	def run(self):
		print self.cmd
		os.system(self.cmd)


class reconstructall(luigi.Task):
	df=pd.read_csv("statetable")
	def output(self):
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
	def requires(self):
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
		Rib = []
		Sec = []
		Cha = []
		Ses = []
		for (rib,sess,sect,ch) in uniq_sections:	
			if (rib == 0) & (sess > 0):
				Rib.append(rib)
				Cha.append(ch)
				Sec.append(sect)
				Ses.append(sess)

		return[reconstruction(ribbon = Rib[i], session = Ses[i], section = Sec[i], channel = Cha[i], df = self.df) for i in range (0, len(Rib))]
	def run(self):
		print "Running reconstruction on all sections..."

