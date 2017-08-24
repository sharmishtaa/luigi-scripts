import sys
sys.path.insert(0,'/usr/local/render-python-apps/')
import os
import luigi
import renderapi
from renderapi.transform import AffineModel
import json
from renderapps.module.render_module import RenderModule,RenderParameters
from pathos.multiprocessing import Pool
from functools import partial
import tempfile
import marshmallow as mm
import os
import numpy as np
from RenderTileParameters import RenderTileParameters
import glob
import logging
from renderapi.utils import NullHandler

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())



class RenderTileTarget(luigi.Target):

        def __init__(self,params,tileId):
                self.parameters = params
                #self.parameters['tileId'] = tileId
                self.tileId = tileId
        def exists(self):
			self.parameters['tileId'] = self.tileId
			print self.parameters
			mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
			#mod.run()

			#############intermediate hack for April 2017 run################

			try:
				ts = mod.render.run(renderapi.tilespec.get_tile_spec,self.parameters['input_stack'], self.parameters['tileId'])
				d = ts.to_dict()
				filename = d['mipmapLevels'][0]['imageUrl']
				print filename
				return os.path.isfile(filename)
				#return luigi.LocalTarget(filename)

			except Exception as e:
				logger.error(e)
				return False

			#################################################################

			#############original which bombards the system##################
			#I = mod.render.run(renderapi.image.get_tile_image_data,self.parameters['input_stack'], self.parameters['tileId'])
			#if I is None:
			#		return False
			#else:
			#	return True
			##################################################################

			#NOTE: Have put in a github request to validate the existence of files in a better way#
