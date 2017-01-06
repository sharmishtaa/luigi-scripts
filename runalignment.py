import luigi
from stitching import stitch_section
from runluigi import runluigi
import time
import sys
import os
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/celery/')
sys.path.insert(0,'/data/array_tomography/ForSharmi/allen_SB_code/MakeAT/')
from celery import Celery
from tasks import run_celerycommand
import pandas as pd
import argparse
#sys.path.insert(0,'/nas/data/M246930_Scnn1aTdt_3/scripts/')
#from run_alignment_setup import run_alignment_setup1
import tifffile
import glob
import numpy
import shutil

def calculateminmax(filename):
	img0 = tifffile.imread(filename)
	#find mean and standard deviation of all values
	m = numpy.mean(img0)
	s = numpy.std(img0)
	range0 = round(m-s)
	range1 = round(m+s)
	return [range0, range1]


def updateimportfiles(directory):
	curdir = os.getcwd()
	os.chdir(directory)
	print directory
	shutil.move("import.txt","importorig.txt")
	outfile = open("import.txt","w")
	with open("importorig.txt", "r") as ins:
		for line in ins:
			print line
			strings = line.split()
			[range0,range1] = calculateminmax(strings[0])
			strings[6] = str(range0)
			strings[7] = str(range1)
			newline = ""
			for j in range(0, len(strings)):
				newline = newline + strings[j] + " "
			newline = newline + "\n"
			print newline
			print "\n"
			outfile.write(newline)
				
	outfile.close()
	os.chdir(curdir)



def runalignmentqc(setup, align, lssorted, exitlssorted):

	if (setup == True):
		curdir = os.getcwd()
		if os.path.isdir(os.getcwd() + "/../processed/jobs"):
			print "Jobs already exists!"
		else:
			

			print lssorted
			print exitlssorted
			print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
			if (lssorted == True):			
				cmd1 = "find %s/../processed/stitched_files/ -name '*rib*sess0001*DAPI*.tif' | sort > ls-sorted.txt"%(curdir)
				print cmd1
				os.system("rm %s/ls-sorted.txt"%(curdir))
				os.system(cmd1)

			if (exitlssorted == True):
				print "Exiting!!!!!!!!!!!!!!"
				sys.exit()

			cmd2 = "/data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/make-import-overlap ls-sorted.txt 100 1000 6000 1"
			print cmd2
			os.system(cmd2)
	
			dirnames = glob.glob('../processed/*-*')
			for i in range(0,len(dirnames)):
				updateimportfiles(dirnames[i])


			cmd3 = "/data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/create-jobs"	
			print cmd3
			os.system(cmd3)
			time.sleep(3)

	if (align == True):
		for x in range(0,1):
			cmd = "PYTHONPATH='' luigi align3d --module align3d  --workers 4"
			print cmd
			os.system(cmd)	
			time.sleep(900)
			

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description = "Run Alignment Only")
	parser.add_argument('--setup',action='store_true',default=False)
	parser.add_argument('--align',action='store_true',default=False)
	parser.add_argument('--lssorted',action='store_true',default=False)
	parser.add_argument('--exitlssorted',action='store_true',default=False)
    	args = parser.parse_args()
	

	runalignmentqc(args.setup, args.align, args.lssorted, args.exitlssorted)

	curdir = os.getcwd()

	#align intersections
	#os.system("sh /data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/create-align-overlapping-projects-pair-jobs ls-sorted.txt 100")
	#os.system("sh runme_overlapping.sh")

	#apply intersection alignments - very quick
	#os.system("sh /data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/create-apply-aligned-overlapping-projects-job ls-sorted.txt 100")
	#os.system ("cd ../processed/jobs/")
	#os.system ("sh apply-aligned-overlapping-projects")
	#os.system ("cd ../../scripts/")

	#export - this takes a while
	os.system("sh /data/array_tomography/ForSharmi/allen_SB_code/parallel-elastic-alignment-master/create-export-jobs ls-sorted.txt 100")
	os.system("sh runme_export.sh")
    
