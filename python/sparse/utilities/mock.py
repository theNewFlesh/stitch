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
.. module:: mock
	:date: 12.29.2013
	:platform: Unix
	:synopsis: Library of mock objects

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import time
import random
from sparse.utilities.utils import Base
from sparse.utilities.errors import *
# ------------------------------------------------------------------------------

class MockDatabase(Base):
	def __init__(self, name=None):
		super(MockDatabase, self).__init__(name=name)
		self._cls = 'MockDatabase'

	@property
	def jobs(self):
		raise EmptyFunction('Please define this method in your subclass')
# ------------------------------------------------------------------------------

class qb(MockDatabase):
	def __init__(self, name=None):
		super(qb, self).__init__(name=name)
		self._cls = 'qb'

	def jobinfo(filters={}, agenda=True, subjobs=False):
		jobs = [
		{'id': 123000, 'name':      'vfx_herc_nuke_shot01_v001', 'priority':    5, 'timestart': 1, 'timecomplete': 2},
		{'id': 123001, 'name':      'vfx_herc_nuke_shot01_v002', 'priority':  100, 'timestart': 1, 'timecomplete': 2},
		{'id': 123002, 'name':   'vfx_jupiter_vray_shot01_v005', 'priority':  100, 'timestart': 1, 'timecomplete': 2},
		{'id': 123003, 'name':    'vfx_tyrant_nuke_shot01_v001', 'priority':  100, 'timestart': 1, 'timecomplete': 2},
		{'id': 123004, 'name':      'vfx_herc_maya_shot01_v010', 'priority':  100, 'timestart': 1, 'timecomplete': 2},
		{'id': 123005, 'name':      'vfx_herc_nuke_shot03_v004', 'priority':  500, 'timestart': 1, 'timecomplete': 2},
		{'id': 123006, 'name':      'vfx_herc_nuke_shot22_v007', 'priority':  500, 'timestart': 1, 'timecomplete': 2},
		{'id': 123007, 'name':   'vfx_jupiter_nuke_shot05_v001', 'priority':  500, 'timestart': 1, 'timecomplete': 2},
		{'id': 123008, 'name': 'vfx_tyrant_houdini_shot01_v002', 'priority': 2000, 'timestart': 1, 'timecomplete': 2},
		{'id': 123009, 'name':      'vfx_herc_maya_shot56_v015', 'priority': 2500, 'timestart': 1, 'timecomplete': 2},
		{'id': 123010, 'name':      'vfx_herc_maya_shot01_v010', 'priority': 2501, 'timestart': 1, 'timecomplete': 2},
		{'id': 123011, 'name':      'vfx_herc_nuke_shot03_v004', 'priority': 2501, 'timestart': 1, 'timecomplete': 2},
		{'id': 123012, 'name':      'vfx_herc_nuke_shot22_v007', 'priority': 2501, 'timestart': 1, 'timecomplete': 2},
		{'id': 123013, 'name':   'vfx_jupiter_nuke_shot05_v001', 'priority': 2501, 'timestart': 1, 'timecomplete': 2},
		{'id': 123014, 'name': 'vfx_tyrant_houdini_shot01_v002', 'priority': 3000, 'timestart': 1, 'timecomplete': 2},
		{'id': 123015, 'name':      'vfx_herc_maya_shot56_v015', 'priority': 3000, 'timestart': 1, 'timecomplete': 2}
		]

		if agenda:
			base_time = time.time()
			for i, job in enumerate(jobs):
				frames = []
				for n in range(1, 10):
					frame = {}
					time_ = base_time + (i * 500) + (n * 50)
					frame['timestart'] = time_
					frame['timecomplete'] = time_ + random.randrange(1000, 1500)
					frame['status'] = 'complete'
					frames.append(frame)
				job['agenda'] = frames

		if subjobs:
			base_time = time.time()
			for i, job in enumerate(jobs):
				subjobs = []
				for n in range(1, 10):
					subjob = {}
					subjob['name'] = 'subjob_' + str(n).zfill(3)
					subjob['id'] = 123111 + (i * 100) + n
					time_ = base_time + (i * 500) + (n * 50)
					subjob['timestart'] = time_
					subjob['timecomplete'] = time_ + random.randrange(1000, 1500)
					subjob['status'] = 'complete'
					subjobs.append(subjob)
				job['subjobs'] = subjobs

		return jobs

	@property
	def jobs(self):
		return self.jobinfo()
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['MockDatabase', 'qb']

if __name__ == '__main__':
	main()