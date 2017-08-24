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


#An example set of parameters for this module
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

class TemplateParameters(RenderParameters):
    example = mm.fields.Str(required=True,default=None,
        metadata={'description':'an example'})


class Template(RenderModule):
    def __init__(self,schema_type=None,*args,**kwargs):
        if schema_type is None:
            schema_type = TemplateParameters
        super(Template,self).__init__(schema_type=schema_type,*args,**kwargs)
    def run(self):
        print mod.args

if __name__ == "__main__":
    #process the command line arguments
    mod = RenderModule(schema_type=TemplateParameters,input_data=example_parameters)
    mod.run()
    #get the z values in the stack
    zvalues = mod.render.run(renderapi.stack.get_z_values_for_stack,mod.args['input_stack'])
    print zvalues

    #mod = Template()
    #mod.run()
