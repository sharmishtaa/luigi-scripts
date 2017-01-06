import luigi
import pandas as pd
from checkfileexists import checkfileexists
import os
from extractfeatures import extractfeatures
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd())

class applyregister(luigi.Task):
	ribbon = luigi.IntParameter()
	session = luigi.IntParameter()
	section = luigi.IntParameter()	
	channame = luigi.Parameter()
	refsession = luigi.IntParameter()
	cmd = ""
	
	def output(self):
	#	if not os.path.exists("../processed/registration_transformspec/"):
	#		os.mkdir ("../processed/registration_transformspec/")
	#	if not os.path.exists("../processed/registration_tilepec/"):
	#		os.mkdir ("../processed/registration_tilespec/")

		outputtilespec = "../processed/registration_tilespec/%s_%01d_rib%04dsess%04dsect%04d.json"%(self.channame,self.session,self.ribbon, self.session, self.section)				

		return [luigi.LocalTarget(outputtilespec)]

	def requires(self):

		self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.ApplyRegistration "
		self.cmd = self.cmd + " --inputtilespec ../processed/stitched_tilespec/%s_rib%04dsess%04dsect%04d.json"%(self.channame,self.ribbon,self.session,self.section)
		self.cmd = self.cmd + " --registeredtilespec ../processed/registration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.session,self.ribbon,self.session,self.section)
		self.cmd = self.cmd + " --outputJson ../processed/registration_tilespec/%s_rib%04dsess%04dsect%04d.json"%(self.channame,self.ribbon,self.session,self.section)

		return []

	def run(self):
		print self.cmd
		os.system(self.cmd)

class applyregistersessions(luigi.Task):
	refsession = luigi.IntParameter()
	df =pd.read_csv("statetable")
	def output(self):
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
	def requires(self):
		uniq_sections = self.df.groupby(['ribbon','session','section','ch']).groups.keys()
		R = []
		C = []
		S = []
		CHAN = []
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
			CHAN.append(channame)
		return[applyregister(ribbon = R[i], section = C[i], session = S[i], channame = CHAN[i], refsession = self.refsession) for i in range (0, len(R))]		
	def run(self):
		print "running registration on all sections"


