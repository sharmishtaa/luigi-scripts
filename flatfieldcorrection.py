import luigi
import os
from checkfileexists import checkfileexists
from createmedian import createmedian
from focusmapping import focusmapping
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
from celery import Celery
from tasks import run_celerycommand

class flatfieldcorrection(luigi.Task):
   flatfieldcorrectedfile = luigi.Parameter()
   channelname=luigi.Parameter()
   channelnum= luigi.Parameter()
   inputfile=luigi.Parameter()
   df = luigi.Parameter()
   rootdir = luigi.Parameter()
  
   row = luigi.Parameter()

   def requires(self):
	if (self.channelname[:4] == "DAPI"):
		medianfile = self.rootdir+"/processed/median/Median_DAPI.tif"
	else:
		medianfile = self.rootdir+"/processed/median/Median_" + self.channelname + ".tif"

	zimages = self.df[(self.df['ribbon']==self.row.loc['ribbon']) & (self.df['ch']==self.channelnum) & (self.df['session']==self.row.loc['session']) & (self.df['frame']==self.row.loc['frame'])  & (self.df['section']==self.row.loc['section'])]
	self.Z = len(zimages)

	if (self.Z > 1) :
		return [createmedian(channelname=self.channelname,channelnum=self.channelnum,df=self.df),focusmapping(df=self.df, row=self.row, rootdir=self.rootdir)]
	else:
		return [checkfileexists(self.inputfile), createmedian(channelname=self.channelname,channelnum=self.channelnum,df=self.df,rootdir=self.rootdir)]
		
	
   def output(self):
	print "Printing file name: "
	print self.flatfieldcorrectedfile
	return luigi.LocalTarget(self.flatfieldcorrectedfile)
	
   def run(self):
	print "flat field correction"
	self.cmd = "python /data/array_tomography/ForSharmi/allen_SB_code/MakeAT/flatfield_correct.py "
	if (self.Z > 1):	
		focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(self.row['ribbon'],self.row['session'],self.channelname, self.channelname, self.row['section'], self.row['frame'])
		self.cmd = self.cmd + " --inputImage " + focusmappedfile		
	else:
		self.cmd = self.cmd + " --inputImage " + self.inputfile
	self.cmd = self.cmd + " --outputImage " + self.flatfieldcorrectedfile
	if (self.channelname[:4] == "DAPI"):
		self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_DAPI.tif"%self.rootdir
	else:
		self.cmd = self.cmd + " --flatfieldStandardImage %s/processed/median/Median_"%self.rootdir + self.channelname + ".tif"
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
    rootdir=luigi.Parameter()

    def output(self):
	return [ luigi.LocalTarget(self.flatfieldfilenames[index]) for index in range (0,len(self.flatfieldfilenames)) ]
    def requires(self):	
	return [ flatfieldcorrection(flatfieldcorrectedfile=self.flatfieldfilenames[index], channelname=self.channelname, channelnum=self.channelnum, inputfile=self.inputfilenames[index], df=self.df, row=self.allrows[index],rootdir=self.rootdir) for index in range (0,len(self.flatfieldfilenames)) ]

    def run(self):
	print "Running flat field correction on all files in section"

