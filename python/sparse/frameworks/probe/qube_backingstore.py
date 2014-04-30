#! /usr/bin/env python
# Alex Braun 04.13.2014

# ------------------------------------------------------------------------------
# The MIT License (MIT)

# Copyright (c) 2014 Alex Braun

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# ------------------------------------------------------------------------------

'''
.. module:: qube_backingstore
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Qube BackingStore for interfacing with Probe API

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import subprocess
from datetime import datetime
from pandas import DataFrame
from sparse.frameworks.probe.backingstore import BackingStore
from sparse.utilities.mock import qb
# ------------------------------------------------------------------------------

class QubeBackingStore(BackingStore):

	def __init__(self, name=None):
		super(QubeBackingStore, self).__init__(name=name)
		self._cls = 'QubeBackingStore'

		self._db = qb()
		self._data = None
		self._results = None
		self._instruction_map = {}

	@property
	def source_data(self):
		# cmd  = '"'
		# cmd += 'import json; '
		# cmd += 'import qb; '
		# cmd += "jobs = qb.jobinfo(filters={'status':'running'}, subjobs=True'); "
		# cmd += 'jobs = [dict(job) for job in jobs]; '
		# cmd += 'print json.dumps(jobs)'
		# cmd += '"'
		# jobs = subprocess.Popen(['python2.6', '-c', cmd], stdout=subprocess.PIPE)
		# jobs = json.loads(jobs)[0]

		# Won't work until Qube for python 2.7 comes out
		jobs = [dict(job) for job in self._db.jobinfo(subjobs=True)]

		for job in jobs:
			subjobs = DataFrame(job['subjobs'])
			stdv = subjobs['timecomplete'] - subjobs['timestart']
			stdv = str(stdv.std()).split('.')
			stdv = float(stdv[0] + '.' + stdv[1][:3])
			job['frames_stdv'] = stdv
		return jobs
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['QubeBackingStore']

if __name__ == '__main__':
	main()