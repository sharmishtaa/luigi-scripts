import os
from renderapi.tilespec import TileSpec
import json
import numpy as np





if __name__ == '__main__':


	inputfile="/nas/data/M247514_Rorb_1/processed/registration_tilespec/DAPI_3_rib0001sess0003sect0040_old.json"
	outputfile = "/nas/data/M247514_Rorb_1/processed/registration_tilespec/DAPI_3_rib0001sess0003sect0040.json"

	with open(inputfile) as json_data:
		stts = json.load(json_data)

	x = []
	y = []

	for ts in stts:
		tok = ts['transforms']['specList'][0]['dataString'].split()
		if float(tok[4]) == 0 :
			print tok[4]
		else:
			x.append(float(tok[4]))
	
		if float(tok[5]) == 0 :
			print tok[5]
		else:
			y.append(float(tok[5]))


	medianx = np.median(x)
	mediany = np.median(y)


	print "This is median: "
	print "%f %f"%(medianx,mediany)
	for ts in stts:
	
		tok = ts['transforms']['specList'][0]['dataString'].split()
		diffx = abs(float(tok[4]) - medianx)
		diffy = abs(float(tok[5]) - mediany)

		if diffx >=3 :
			tok[4] = str(medianx)
		if diffy >=3 :
			tok[5] = str(mediany)
		ds = "%s %s %s %s %s %s"%(tok[0],tok[1],tok[2],tok[3],tok[4],tok[5])
	
		ts['transforms']['specList'][0]['dataString']=ds

		print ts['transforms']['specList'][0]['dataString']
		

	json_text=json.dumps([t for t in stts],indent=4)
	fd=open(outputfile, "w")
	fd.write(json_text)
	fd.close()

