#!/pipeline/anaconda/bin/python

#import os
#import sys
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
	parser.add_argument('--session',nargs=1,help="session number")
	parser.add_argument('--section',nargs=1,help="section number")
	parser.add_argument('--ribbon',nargs=1,help="ribbon number")
	parser.add_argument('--outfile',nargs=1,help="outputfile")
	parser.add_argument('--slackchannel',nargs=1,help="slackchannel")
	
	args = parser.parse_args()
	print args.owner[0]
	print args.projectName[0]

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
	parameters['input_stack'] = 'Acquisition_DAPI_%s'%args.session[0]
	z = int(args.ribbon[0])*100+int(args.section[0])	

	print "This is z: ",z
	mod = RenderModule(schema_type=RenderTileParameters,input_data=parameters,args=[])
	mod.run()
	I = mod.render.run(renderapi.image.get_section_image,parameters['input_stack'], z, 0.05)
	im = Image.fromarray(I)
	im.save(args.outfile[0])
	
	#upload to slack
	
	slack = Slacker('xoxb-171018196033-xRN2g7DdUQmeEORrrE7TwQNd')
	#slack.chat.post_message('#imaging',"Thank you ;)")
	slack.files.upload(args.outfile[0],channels=[args.slackchannel[0]])
