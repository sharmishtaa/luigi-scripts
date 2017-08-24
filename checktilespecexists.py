#!/usr/bin/env python

import sys
sys.path.insert(0,'/usr/local/render-python-apps/')
import renderapi
from renderapi.transform import AffineModel
import json
from render_module import RenderModule,RenderParameters
import JsonRunnableModule
from pathos.multiprocessing import Pool
from functools import partial
import tempfile
import marshmallow as mm
import os
import numpy as np
import luigi

class checktilespecexists(luigi.Task):
	
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
		}
		
    
	def output(self):
		
		class RenderConnectParameters(RenderParameters):
			input_stack = mm.fields.Str(required=True,metadata={'description':'stack to check'})
		#j = RenderModule(RenderConnectParameters,example_parameters)
		mod = RenderModule(RenderConnectParameters,self.example_parameters)
		#mod.run()
		#get the z values in the stack
		#zvalues = mod.render.run(renderapi.stack.get_z_values_for_stack,mod.args['input_stack'])
		#zvalues = np.array(zvalues)
		#print zvalues
		return luigi.LocalTarget(self.filename)

