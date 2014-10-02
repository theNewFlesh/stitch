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
.. module:: qb
	:date: 12.29.2013
	:platform: Unix
	:synopsis: Mock qb library

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import time
from random import *
# ------------------------------------------------------------------------------

def setsupervisor(name):
	pass

def jobinfo(filters={}, agenda=True, subjobs=False, fields=None, callbacks=None, id=None, status=None):
	jobs = [
	{'id': 123000, 'name':      'vfx_herc_nuke_shot01_v001', 'priority':    5, 'status':  'running', 'reservations':  'processors=16+, memory=16000', 'pgrp': 999999    },
	{'id': 123001, 'name':      'vfx_herc_nuke_shot01_v002', 'priority':  100, 'status':  'running', 'reservations':  'processors=16+, memory=16000', 'pgrp': 999999    },
	{'id': 123002, 'name':   'vfx_jupiter_vray_shot01_v005', 'priority':  100, 'status':   'failed', 'reservations':  'processors=16, memory=16000+', 'pgrp': 999999    },
	{'id': 123003, 'name':    'vfx_tyrant_nuke_shot01_v001', 'priority':  100, 'status':  'running', 'reservations': 'processors=16+, memory=16000+', 'pgrp': 999999    },
	{'id': 123004, 'name':      'vfx_herc_maya_shot01_v010', 'priority':  100, 'status': 'complete', 'reservations':  'processors=16+, memory=16000', 'pgrp': 999999    },
	{'id': 123005, 'name':      'vfx_herc_nuke_shot03_v004', 'priority':  500, 'status':  'running', 'reservations':  'processors=16+, memory=8000+', 'pgrp': 999999    },
	{'id': 123006, 'name':      'vfx_herc_nuke_shot22_v007', 'priority':  500, 'status':  'running', 'reservations':    'processors=24, memory=8000', 'pgrp': 999999    },
	{'id': 123007, 'name':   'vfx_jupiter_nuke_shot05_v001', 'priority':  500, 'status':  'pending', 'reservations':    'processors=24, memory=8000', 'pgrp': 999999    },
	{'id': 123008, 'name': 'vfx_tyrant_houdini_shot01_v002', 'priority': 2000, 'status':  'pending', 'reservations':  'processors=24+, memory=8000+', 'pgrp': 999999    },
	{'id': 123009, 'name':      'vfx_herc_maya_shot56_v015', 'priority': 2500, 'status':  'running', 'reservations':   'processors=24+, memory=8000', 'pgrp': 999999    },
	{'id': 123010, 'name':      'vfx_herc_maya_shot01_v010', 'priority': 2501, 'status':   'failed', 'reservations':   'processors=24, memory=32000', 'pgrp': 999999    },
	{'id': 123011, 'name':      'vfx_herc_nuke_shot03_v004', 'priority': 2501, 'status':  'pending', 'reservations':   'processors=32, memory=32000', 'pgrp': 999999    },
	{'id': 123012, 'name':      'vfx_herc_nuke_shot22_v007', 'priority': 2501, 'status':  'running', 'reservations':   'processors=32, memory=32000', 'pgrp': 999999    },
	{'id': 123013, 'name':   'vfx_jupiter_nuke_shot05_v001', 'priority': 2501, 'status':  'running', 'reservations':   'processors=32, memory=32000', 'pgrp': 999999    },
	{'id': 123014, 'name': 'vfx_tyrant_houdini_shot01_v002', 'priority': 3000, 'status':  'running', 'reservations':  'processors=24, memory=32000+', 'pgrp': 999999    },
	{'id': 123015, 'name':      'vfx_herc_maya_shot56_v015', 'priority': 3000, 'status':   'failed', 'reservations':  'processors=24+, memory=8000+', 'pgrp': 999999    }
	]     

	for item in jobs:
		item['todotally'] = {'failed': randint(0, 100), 
							 'complete': randint(0, 100),
							 'running': randint(0, 100),
							 'pending': randint(0, 100)}
		item['todo'] = 100
		item['timestart'] = time.time() - (1000 * randint(1, 10))
		item['timecomplete'] = time.time() - (10 * randint(0, 5))

	if agenda:
		base_time = time.time() - (1000 * randint(1, 5))
		for i, job in enumerate(jobs):
			frames = []
			for n in range(1, 10):
				frame = {}
				frame['lastupdate'] = time.time()
				time_ = base_time + (i * 500) + (n * 50)
				frame['timestart'] = time_
				frame['timecomplete'] = time_ + randrange(1000, 1500)
				frame['timecumulative'] = 99
				stat = 'complete'
				if n < 6:
					stat = 'running'
				frame['status'] = stat
				frame['retry'] = -1
				frame['name'] = n
				frame['host'] = 'render_node_' + str(n)
				frame['pid'] = 'test'
				frame['subid'] = '99.' + str(n)
				frame['count'] = 10
				frame['id'] = 99
				frame['retrydelay'] = 99
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
				subjob['timecomplete'] = time_ + randrange(1000, 1500)
				subjob['status'] = 'complete'
				subjobs.append(subjob)
			job['subjobs'] = subjobs

	return jobs

def set_priority(job_ids, priority):
	for job_id in job_ids:
		print job_id, 'set to', priority
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['setsupervisor', 'jobinfo', 'set_priority']

if __name__ == '__main__':
	main()