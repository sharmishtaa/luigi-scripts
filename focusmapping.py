

import luigi
import os
import pandas as pd
from checkfileexists import checkfileexists
import sys
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
from celery import Celery
from tasks import run_celerycommand
import time

class focusmapping_channel0 (luigi.Task):
	df = luigi.Parameter()
	row = luigi.Parameter()
	focusmappedfile = ''
	mapfile = ''
	mapimages = []
	ch = 10000
	def output (self):
		rib = self.row.loc['ribbon']
		ch = self.row.loc['ch']
		session = self.row.loc['session']
		section = self.row.loc['section']
		frame = self.row.loc['frame']

		self.map_images = self.df[(self.df['ribbon']==rib) & (self.df['ch']==0) & (self.df['session']==session) & (self.df['section']==section) &  (self.df['frame']==frame)]		
		row0= self.map_images.iloc[0]
		[directory, sep, filename] = row0.loc['full_path'].rpartition('/')
		[directory_up, sep, channame] = directory.rpartition('/')


		#[directory, sep, filename] = self.row.loc['full_path'].rpartition('/')
		#[directory_up, sep, channame] = directory.rpartition('/')
		self.focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(rib,session,channame, channame, section,frame)
		self.mapfile='../processed/maps/Ribbon%04d/Session%04d/S%04dF%04d_map.tif'%(rib,session,section,frame)
		self.ch = ch
		print "OUTPUT OF FOCUSSSSSSSSMAPPPPPPPPPPPPPINGGGGGGGGGGGGG CHANNEL 0!!!!!!!!!!!!!!!!!!!!!!!!!"
		print self.focusmappedfile
		print self.mapfile
		return [luigi.LocalTarget(self.focusmappedfile),luigi.LocalTarget(self.mapfile)]
				
	def requires (self):

		print "FOCUSMAPPINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG"
		print self.row.loc['full_path']
		rib = self.row.loc['ribbon']
		ch = self.row.loc['ch']
		session = self.row.loc['session']
		section = self.row.loc['section']
		frame = self.row.loc['frame']
		[directory, sep, filename] = self.row.loc['full_path'].rpartition('/')
		[directory_up, sep, channame] = directory.rpartition('/')
		self.ch = ch
		self.map_images = self.df[(self.df['ribbon']==rib) & (self.df['ch']==0) & (self.df['session']==session) & (self.df['section']==section) &  (self.df['frame']==frame)]		
		row0= self.map_images.iloc[0]
		[directory, sep, filename] = row0.loc['full_path'].rpartition('/')
		[directory_up, sep, channame] = directory.rpartition('/')
		

		self.focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(rib,session,channame, channame, section,frame)
		self.mapfile='../processed/maps/Ribbon%04d/Session%04d/S%04dF%04d_map.tif'%(rib,session,section,frame)
		return [checkfileexists(filename=row1.loc['full_path']) for index,row1 in self.map_images.iterrows() ]

	def run (self):	
		cmd = "python /data/array_tomography/ForSharmi/allen_SB_code/MakeAT/calc_extend_focus.py "
		cmd = cmd + " --outputImage " + self.focusmappedfile
		cmd = cmd + " --focusMap " + self.mapfile
		cmd = cmd + " --inputImages "
		for index, row1 in self.map_images.iterrows():
			cmd = cmd + " " + row1.loc['full_path']
		cmd = cmd + " --zOrder "
		for index, row1 in self.map_images.iterrows():
			cmd = cmd + " %d"%(row1.loc['zstack'])
		print cmd
		os.system(cmd)
		
		#result = run_celerycommand.apply_async(args=[cmd,os.getcwd()])
		#while not result.ready():
		#	print result.ready()
		#	print "1 second"
		#	time.sleep(1)
			



class focusmapping (luigi.Task):
	df = luigi.Parameter()
	row = luigi.Parameter()
	focusmappedfile = ''
	mapfile = ''
	mapimages = []
	ch = 10000
	def output (self):
		rib = self.row.loc['ribbon']
		ch = self.row.loc['ch']
		session = self.row.loc['session']
		section = self.row.loc['section']
		frame = self.row.loc['frame']
		[directory, sep, filename] = self.row.loc['full_path'].rpartition('/')
		[directory_up, sep, channame] = directory.rpartition('/')
		self.focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(rib,session,channame, channame, section,frame)
		self.ch = ch
		return luigi.LocalTarget(self.focusmappedfile)
				
	def requires (self):
		print "FOCUSMAPPINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG !!!!!!!!!!!"
		print self.row.loc['full_path']
		rib = self.row.loc['ribbon']
		ch = self.row.loc['ch']
		session = self.row.loc['session']
		section = self.row.loc['section']
		frame = self.row.loc['frame']
		[directory, sep, filename] = self.row.loc['full_path'].rpartition('/')
		[directory_up, sep, channame] = directory.rpartition('/')
		self.ch = ch
		self.map_images = self.df[(self.df['ribbon']==rib) & (self.df['ch']==ch) & (self.df['session']==session) & (self.df['section']==section) &  (self.df['frame']==frame)]		
		self.focusmappedfile='../processed/focusmappeddata/Ribbon%04d/Session%04d/%s/%s_S%04d_F%04d_Z00.tif'%(rib,session,channame, channame, section,frame)
		self.mapfile='../processed/maps/Ribbon%04d/Session%04d/S%04dF%04d_map.tif'%(rib,session,section,frame)
			
		#return [focusmapping_channel0(df=df, row=row), checkfileexists(filename=row.loc['full_path']) for index,row in map_images.iterrows() ]
		return [focusmapping_channel0(df=self.df, row=self.row)]

	def run (self):	
		if (self.ch > 0):
			cmd = "python /data/array_tomography/ForSharmi/allen_SB_code/MakeAT/apply_focus_map.py "
			cmd = cmd + " --outputImage " + self.focusmappedfile
			cmd = cmd + " --focusMap " + self.mapfile
			cmd = cmd + " --inputImages "
			for index, row1 in self.map_images.iterrows():
				cmd = cmd + " " + row1.loc['full_path']
			cmd = cmd + " --zOrder "
			for index, row1 in self.map_images.iterrows():
				cmd = cmd + " %d"%(row1.loc['zstack'])
			print cmd
			os.system(cmd)
			#result = run_celerycommand.apply_async(args=[cmd,os.getcwd()])

