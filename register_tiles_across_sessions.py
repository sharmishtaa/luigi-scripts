import luigi
import pandas as pd
from checkfileexists import checkfileexists
import os
from extractfeatures import extractfeatures
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,os.getcwd())

class register(luigi.Task):
	ribbon = luigi.IntParameter()
	session = luigi.IntParameter()
	section = luigi.IntParameter()	
	refsession = luigi.IntParameter()
	cmd = ""
	
	def output(self):
		#if not os.path.exists("../processed/registration_transformspec/"):
		#	os.mkdir ("../processed/registration_transformspec/")
		#if not os.path.exists("../processed/tileregistration_tilepec/"):
		#	os.mkdir ("../processed/tileregistration_tilespec/")

		outputtilespec = "../processed/tileregistration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.session,self.ribbon, self.session, self.section)				
		#outputtransform = "../processed/registration_transformspec/rib%04dsess%04dsect%04d.json"%(self.ribbon, self.refsession, self.section)

		print outputtilespec
		#print outputtransform
		return luigi.LocalTarget(outputtilespec)

	def requires(self):
		
		inputtilespec = "../processed/stitched_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.session,self.ribbon, self.session, self.section)
		referencetilespec = "../processed/stitched_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.refsession,self.ribbon, self.refsession, self.section)
		outputtilespec = "../processed/tileregistration_tilespec/DAPI_%01d_rib%04dsess%04dsect%04d.json"%(self.session,self.ribbon, self.session, self.section)
		#outputtransform = "../processed/registration_transformspec/rib%04dsess%04dsect%04d.json"%(self.ribbon, self.session, self.section)
		refID = "rib%04dsess%04dsect%04d_to_session%01d"%(self.ribbon, self.session, self.section,self.refsession)

		self.cmd = "java -cp /home/sharmishtaas/allen_SB_code/render/render-app/target/render-app-0.3.0-SNAPSHOT-jar-with-dependencies.jar:/home/sharmishtaas/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar org.janelia.alignment.RegisterTiles2Section --inputtilespec "
		self.cmd = self.cmd + inputtilespec + " --referencetilespec " + referencetilespec + " --outputtilespec " + outputtilespec + " --referenceID " + refID 
		return []
	def run(self):
		print self.cmd
		os.system(self.cmd)

class registersessions(luigi.Task):
	refsession = luigi.IntParameter()
	df =pd.read_csv("statetable")
	def output(self):
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
	def requires(self):
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
		R = []
		C = []
		S = []
		for (rib,sess,sect) in uniq_sections:	
			#if (sess == 2) & (rib == 1) & (sect == 0):
			if (sess > self.refsession) | (sess < self.refsession):
				R.append(rib)
				C.append(sect)
				S.append(sess)
		return[register(ribbon = R[i], section = C[i], session = S[i], refsession = self.refsession) for i in range (0, len(R))]		
	def run(self):
		print "running registration on all sections"

