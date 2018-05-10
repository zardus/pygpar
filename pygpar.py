import subprocess
import os

class PP(object):
	"""
	The PP (Python Parallel) class wraps GNU parallel to execute commands.
	"""

	def __init__(self, cmd, jobs=None, eta=True, colsep='%COLSEP%', verbose=False, filter_exists=False, **kwargs):
		self.cmd = [ 'parallel' ]
		if colsep:
			self.cmd += [ '-C', colsep ]
		if eta:
			self.cmd += [ '--eta' ]
		if verbose:
			self.cmd += [ '-v' ]
		if jobs is not None:
			self.cmd += [ '-j', str(jobs) ]
		self.cmd += [ cmd ]
		self.colsep = colsep
		self.process = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, **kwargs)
		self.filter_exists = filter_exists
		self.n = 0

	def _to_line(self, *args):
		if self.filter_exists and os.path.exists(args[0]):
			return ""
		else:
			return self.colsep.join(args)+'\n'

	def queue(self, *args):
		s = self._to_line(*args)
		if not s:
			return None

		self.process.stdin.write(s)
		self.process.stdin.flush()
		self.n += 1
		return self.n

	def queue_list(self, lst):
		s = ''.join(
			(self._to_line(*j) if type(j) in (list, tuple) else self._to_line(j))
			for j in lst
		)
		self.process.stdin.write(s)
		self.process.stdin.flush()
		old_n = self.n
		self.n += len(lst)
		return range(old_n+1, self.n+1)

	def queue_iter(self, it):
		return [ (self.queue(*j) if type(j) in (list, tuple) else self.queue(j)) for j in it ]

	def wait(self):
		self.process.stdin.close()
		self.process.wait()

	def __enter__(self):
		print "ENTERING:", self.cmd
		return self
	def __exit__(self, *args):
		print "... EXITING"
		self.wait()
