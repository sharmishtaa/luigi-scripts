
import luigi


class checkfileexists(luigi.Task):

    filename = luigi.Parameter()

    def output(self):
		return luigi.LocalTarget(self.filename)


if __name__=='__main__':
	luigi.run()
