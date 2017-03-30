
import sys
sys.path.insert(0,'/usr/local/render-python-apps/')

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
from apply_transforms_by_tileId import ApplyTransforms 
from apply_global_affine_to_stack import ApplyAffineParameters

class RenderTarget(luigi.Target):
	example_parameters = {
		"render":{
        	"host":"ibs-forrestc-ux1",
        	"port":8080,
        	"owner":"Forrest",
        	"project":"M247514_Rorb_1",
        	"client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
    		},
    	'input_stack':'EM_Site4_stitched',
    	'output_stack':'EM_Site4_stitched',
    	'M00':1,
    	'M10':0,
    	'M01':0,
    	'M11':1,
    	'B0':2*15294,
    	'B1':-2*1515,
    	'pool_size':20
        }
	def exists(self):
		mod = RenderModule(schema_type=ApplyAffineParameters,input_data=self.example_parameters,args=[])
                mod.run()
		I = mod.render.run(renderapi.image.get_tile_image_data,'ACQ488_AFL', '1017')
		if I is None:
		#if 1==0:
			print "return false..............................................."
			return False
		else:
			print "return true,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
			return True



class checkrenderfileexists(luigi.Task):

    filename = luigi.Parameter()
    example_parameters = {
    "render":{
        "host":"ibs-forrestc-ux1",
        "port":8080,
        "owner":"Forrest",
        "project":"M247514_Rorb_1",
        "client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
    },
    'input_stack':'EM_Site4_stitched',
    'output_stack':'EM_Site4_stitched',
    'M00':1,
    'M10':0,
    'M01':0,
    'M11':1,
    'B0':2*15294,
    'B1':-2*1515,
    'pool_size':20
	}
	
    def output(self):
			
		#mod = RenderModule(schema_type=ApplyAffineParameters,input_data=self.example_parameters,args=[])
		#mod.run()
		#r = renderapi.render.connect("ibs-forrestc-ux1",8080,"Sharmishtaas","M247514_Rorb_1","/pipeline/render/render-ws-java-client/src/main/scripts")
		#zvalues = r.run(renderapi.stack.get_z_values_for_stack,'ACQ488_AFL')
		#print zvalues
		
		#I = r.run(renderapi.image.get_tile_image_data,'ACQ488_AFL', '1017')
		#if I is None:
		#	print "None only"
		#	return False
		#else:
		#	print "I have something"
		#	return True
		#return luigi.LocalTarget(self.filename)
		tmp = RenderTarget()
		#return luigi.contrib.hdfs.webhdfs_client.WebHdfsClient
		return tmp
		#return RenderTarget(self.example_parameters, "TEST",100)
    def run(self):
		print "Hello"

