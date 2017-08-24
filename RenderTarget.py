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


class RenderTarget(luigi.Target):


        def __init__(self,params):
                self.parameters = params

        def exists(self):
			print self.parameters
			mod = RenderModule(schema_type=RenderTileParameters,input_data=self.parameters,args=[])
			#mod.run()


			#############intermediate hack for April 2017 run################
			#ts = mod.render.run(renderapi.tilespec.get_tile_spec,self.parameters['input_stack'], self.parameters['tileId'])
			inttileId = int(self.parameters['tileId'])
			tileId = "%015d"%inttileId


			tzero = int(tileId[0])
			if tzero > 1:
				pre = str(tzero -self.parameters['session'])
			else:
				pre = ''

			tId = pre +  tileId[1:3]+tileId[7:9]
			print "THIS IS TILE z: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			print tId
			z = int(tId)
			try:
				ts = mod.render.run(renderapi.tilespec.get_tile_specs_from_z,self.parameters['input_stack'],z)
				d = ts[0].to_dict()
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
			#	else:
			#	return True
			##################################################################

			#NOTE: Have put in a github request to validate the existence of files in a better way#
