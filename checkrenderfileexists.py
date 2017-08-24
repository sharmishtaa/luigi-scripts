import sys
sys.path.insert(0,'/usr/local/render-python-apps/')
sys.path.insert(0,'/usr/local/render-python-apps/renderapps/module/')
import os
import luigi
import renderapi
from renderapi.transform import AffineModel
import json
from render_module import RenderModule,RenderParameters
from pathos.multiprocessing import Pool
from functools import partial
import tempfile
import marshmallow as mm
import os
import numpy as np
from RenderTileParameters import RenderTileParameters
import glob
from RenderTarget import RenderTarget


class checkrenderfileexists(luigi.Task):

	parameters = luigi.Parameter()
	
	def output(self):
		
		return RenderTarget(self.parameters)
		
	def run(self):
		print "Hello"

