#sys.path.insert(0,'/usr/local/render-python-apps/renderapps/module/')
#from renderapps.module.render_module import RenderModule,RenderParameters
#from RenderTileParameters import RenderTileParameters


import os
import sys
sys.path.insert(0,'/usr/local/render-python-apps/')
import renderapi
from pathos.multiprocessing import Pool
from render_module import RenderModule,RenderParameters
from json_module import InputFile,InputDir
import marshmallow as mm
#from RenderModule import RenderModule
import logging
#import renderapi
import argparse
from RenderTileParameters import RenderTileParameters
from PIL import Image
from slacker import Slacker

if __name__ == '__main__':
	
	
	parser = argparse.ArgumentParser(description="Download QC image")
	parser.add_argument('--owner',nargs=1,help="name of project owner")
	parser.add_argument('--projectName', nargs=1, help="Project Name")
	parser.add_argument('--stackName',nargs=1,help="Stack name")
	args = parser.parse_args()
	
	parameters = {
			"render":{
			"host":"ibs-forrestc-ux1",
			"port":8080,
			"client_scripts":"/pipeline/render/render-ws-java-client/src/main/scripts"
			}
	}
	parameters['render']['owner'] = args.owner[0]
	parameters['tileId'] = '1000' #dummy  number
	parameters['render']['project'] = args.projectName[0]
	parameters['input_stack'] = args.stackName[0]
	
	mod = RenderModule(schema_type=RenderTileParameters,input_data=parameters,args=[])
	mod.run()
	mod.render.run(renderapi.stack.set_stack_state,args.stackName[0],'COMPLETE')
	
	
