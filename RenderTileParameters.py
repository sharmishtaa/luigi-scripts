#!/usr/bin/env python
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
    'tileId':'1016',
}

class RenderTileParameters(RenderParameters):
    input_stack = mm.fields.Str(required=True,metadata={'description':'stack to connect to'})
    tileId = mm.fields.Str(required=True,metadata={'description':'tileId to get information'})

