import os
import time

def run_alignment_setup():
	if os.path.isdir(os.getcwd() + "/../processed/jobs"):
		print "Jobs already exists!"
	else:
		curdir = os.getcwd()
		cmd1 = "find %s/../processed/stitched_files/ -name '*rib*sess0000*DAPI*.tif' | sort > ls-sorted.txt"%(curdir)
		os.system(cmd1)

		cmd2 = "/data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/make-import-overlap ls-sorted.txt 100 100 2000 1"
		os.system(cmd2)

		cmd3 = "/data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/create-jobs"	
		os.system(cmd3)
		time.sleep(30)

if __name__ == "__main__":
	run_alignment_setup()
