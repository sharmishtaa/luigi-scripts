import luigi
from stitching import stitch_section

def runluigi():

    temp = "Hello How are you"
    #print temp
    #for x in range (0,5):
   # 	print "Run number: %d "%x
    #luigi.run(["--local-scheduler"], main_task_cls=stitch_section)
    luigi.run(main_task_cls=stitch_section)

if __name__ == '__main__':
	runluigi()
