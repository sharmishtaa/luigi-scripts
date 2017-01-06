import luigi
import pandas as pd
from checkfileexists import checkfileexists
import os

class extractfeatures(luigi.Task):
	ribbon = luigi.IntParameter()
    	section = luigi.IntParameter()
    	session = luigi.IntParameter()
	cmd = ""

	def output(self):
		outputfile = "../processed/feature_files/rib%04dsess%04dsect%04d.txt"%(self.ribbon, self.session, self.section)
		return luigi.LocalTarget(outputfile)		
	def requires(self):
		imagefile = "../processed/stitched_files/rib%04dsess%04dsect%04d_DAPI.tif"%(self.ribbon, self.session, self.section)
		outputfile = "../processed/feature_files/rib%04dsess%04dsect%04d.txt"%(self.ribbon, self.session, self.section)
		self.cmd = "java -cp /data/array_tomography/ForSharmi/allen_SB_code/newForrestFijiBento/target/render-0.0.1-SNAPSHOT.jar:/data/array_tomography/ForSharmi/.m2/repository/sc/fiji/Fiji_Plugins/2.0.1/Fiji_Plugins-2.0.1.jar  org.janelia.alignment.ComputeSiftFeaturesFromPath --imageFile "
		self.cmd = self.cmd + imagefile + " --outputFile " + outputfile
		return checkfileexists(imagefile)
	def run(self):
		print "RUNNING feature extraction of one section!"
		print self.cmd
		os.system(self.cmd)


class extractallfeatures(luigi.Task):
	df =pd.read_csv("statetable")
	def output(self):
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
	def requires(self):
		uniq_sections = self.df.groupby(['ribbon','session','section']).groups.keys()
		R = []
		C = []
		S = []
		for (rib,sess,sect) in uniq_sections:	
			R.append(rib)
			C.append(sect)
			S.append(sess)
		return[extractfeatures(ribbon = R[i], section = C[i], session = S[i]) for i in range (0, len(R))]		
	def run(self):
		print "running feature extraction on all sections"
