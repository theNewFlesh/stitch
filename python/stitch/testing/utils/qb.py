import os
import re
import time
from random import *
from stitch.core.utils import round_to
# ------------------------------------------------------------------------------

'''
.. module:: qb
	:platform: Unix
	:synopsis: Mock qb library

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

BASE_TIME = time.time() - (500 * randint(0, 2))
STOPS = [135, 135, 130, 130, 180, 112, 116, 100, 110, 90, 47, 47, 25, 25, 14, 14]
LOGPATH = os.path.join(os.getcwd(), 'stitch/testing/resources/qube_logs')

def gen_jobids():
	jobids = ['124000', '123999', '123205', '123202', '123204', '123066', '124015',
			  '124013', '124011', '124012', '123002', '123000', '123101', '123100',
			  '123096', '123097']
	output = []
	for i in range(0, 3):
		jobset = [x + '_' + str(i) for x in jobids]
		output.extend(jobset)
	return output

JOBIDS = gen_jobids()

def semi_random_list(start, stop, low=500, high=1200):
	output = []
	step = randint(0, 34)
	some_int = randint(low, high)
	long_frames = True
	c = 0
	for n in range(start, stop):
		if step < 3 and long_frames:
			some_int = randint(low * 4, high * 4)
			long_frames = False
		else:
			if c > step:
				some_int = randint(low, high)
				c = 0
			some_int *= (randint(85, 110) / 100.0)
		output.append(some_int)
		c += 1
	return output

def setsupervisor(name):
	pass

def _jobinfo(filters={}, agenda=False, subjobs=False, fields=None, callbacks=False, id=None, status=None):
	def _gen_agenda_frames(start, stop, status='complete', low=500, high=1200):
		for i, job in enumerate(jobs):
			frames = []

			semi = semi_random_list(start, stop, low, high)
			for i, n in enumerate(range(start, stop)):
				some_int = semi[i]
				frame = {}
				frame['lastupdate'] = time.time()
				time_ = BASE_TIME - some_int
				frame['timestart'] = time_
				if status is 'complete':
					frame['timecomplete'] = time_ + some_int
				else:
					frame['timecomplete'] = time.time()
				frame['timecumulative'] = 99
				frame['status'] = status
				frame['retry'] = -1
				frame['name'] = str(n)
				frame['host'] = 'render_node_' + str(n).zfill(4)
				frame['pid'] = n
				frame['subid'] = n
				frame['count'] = stop
				frame['id'] = n
				frame['retrydelay'] = 99
				frames.append(frame)
		return frames

	jobs = [

	{'id': 124000, 'name':  'vfx_eos_houdini_fireVillage_lm01_v002', 'priority': 2000, 'status':  'pending', 'reservations':  'processors=24+, memory=8000+', 'pgrp': 123999, 'label': 'MantraRender',          'callbacks': u'{"triggers": "complete-job-IFD_Generate.Fire"       }' },
	{'id': 123999, 'name':   'vfx_eos_ifdgen_fireVillage_lm01_v002', 'priority': 3000, 'status':  'running', 'reservations':  'processors=24, memory=32000+', 'pgrp': 123999, 'label': 'IFD_Generate.Fire',     'callbacks': u'{"triggers": ""                                     }' },
	{'id': 123205, 'name':           'vfx_chronos_vray_shot01_v005', 'priority':  110, 'status':   'failed', 'reservations':   'processors=24, memory=32000', 'pgrp': 123202, 'label': 'VRAY',                  'callbacks': u'{"triggers": "complete-work-VR_SCENE.Master and complete-job-VR_SCENE.Perspective" }' },
	{'id': 123202, 'name': 'vfx_chronos_vrscene_master_shot01_v005', 'priority':  110, 'status':   'failed', 'reservations':  'processors=16, memory=16000+', 'pgrp': 123202, 'label': 'VR_SCENE.Master',       'callbacks': u'{"triggers": ""                                     }' },
	{'id': 123204, 'name':   'vfx_chronos_vrscene_perp_shot01_v005', 'priority':  110, 'status': 'complete', 'reservations':  'processors=16+, memory=16000', 'pgrp': 123202, 'label': 'VR_SCENE.Perspective',  'callbacks': u'{"triggers": "complete-work-VR_SCENE.Master"        }' },
	{'id': 123066, 'name':              'design_temple_shot56_v015', 'priority': 2500, 'status':  'running', 'reservations':   'processors=24+, memory=8000', 'pgrp': 123066, 'label': 'MayaBatch',             'callbacks': u'{"triggers": ""                                     }' },
	{'id': 124015, 'name':          'vfx_hyperion_maya_shot56_v015', 'priority': 3000, 'status':   'failed', 'reservations':  'processors=24+, memory=8000+', 'pgrp': 123015, 'label': 'VRAY',                  'callbacks': u'{"triggers": ""                                     }' },
	{'id': 124013, 'name':           'vfx_chronos_nuke_shot05_v001', 'priority': 2501, 'status':  'running', 'reservations':   'processors=32, memory=32000', 'pgrp': 123010, 'label': 'Archive',               'callbacks': u'{"triggers": ""                                     }' },
	{'id': 124011, 'name':          'vfx_hyperion_nuke_shot03_v004', 'priority': 2499, 'status':  'failed', 'reservations':   'processors=32, memory=32000', 'pgrp': 123030, 'label': 'New-Nuke-Job',          'callbacks': u'{"triggers": "complete-work-more.bad.data"           }' },
	{'id': 124012, 'name':          'vfx_hyperion_nuke_shot22_v007', 'priority': 2501, 'status':  'running', 'reservations':   'processors=32, memory=32000', 'pgrp': 123012, 'label': 'Nuke',                  'callbacks': u'{"triggers": "complete-work-bad_data"               }' },
	{'id': 123002, 'name':          'vfx_hyperion_nuke_shot01_v001', 'priority':    5, 'status':  'running', 'reservations':  'processors=16+, memory=16000', 'pgrp': 123000, 'label': 'Nuke',                  'callbacks': u'{"triggers": "complete-work-Archive"                }' },
	{'id': 123000, 'name':  'vfx_hyperion_nuke_shot01_v001_archive', 'priority':  100, 'status':  'running', 'reservations': 'processors=16+, memory=16000+', 'pgrp': 123000, 'label': 'Archive',               'callbacks': u'{"triggers": ""                                     }' },
	{'id': 123101, 'name':          'vfx_hyperion_nuke_shot01_v004', 'priority':  100, 'status':  'running', 'reservations':  'processors=16+, memory=16000', 'pgrp': 123100, 'label': 'Nuke',                  'callbacks': u'{"triggers": "complete-work-Archive"                }' },
	{'id': 123100, 'name':          'vfx_hyperion_nuke_shot01_v004', 'priority':  500, 'status':  'running', 'reservations':  'processors=16+, memory=8000+', 'pgrp': 123100, 'label': 'Archive',               'callbacks': u'{"triggers": ""                                     }' },
	{'id': 123096, 'name':             'vfx_atlas_nuke_shot22_v007', 'priority':  500, 'status':  'pending', 'reservations':    'processors=24, memory=8000', 'pgrp': 123096, 'label': 'New.Nuke.Dependency',   'callbacks': u'{"triggers": ""                                     }' },
	{'id': 123097, 'name':             'vfx_atlas_nuke_shot22_v007', 'priority':  500, 'status':   'failed', 'reservations':    'processors=24, memory=8000', 'pgrp': 123096, 'label': 'New-Nuke-Job',          'callbacks': u'{"triggers": "complete-work-New.Nuke.Dependency"    }' }
	]

	for item in jobs:
		failed = randint(0, 20)
		complete = randint(0, 40)
		running = randint(0, 60)
		pending = randint(0, 30)

		item['todotally'] = {'failed': failed,
							 'complete': complete,
							 'running': running,
							 'pending': pending}
		item['todo'] = failed + complete + running + failed
		item['timestart'] = time.time() - (1000 * randint(1, 10))
		item['timecomplete'] = time.time() - (10 * randint(0, 5))

	if agenda:
		for i, job in enumerate(jobs):
			frames = []
			t4 = STOPS[i]
			t1 = int(t4 / 4)
			t2 = t1 + t1
			t3 = t2 + t1

			frames.extend(_gen_agenda_frames(0, t1, 'running', 1200, 3600))

			if randint(0, 4):
				frames.extend(_gen_agenda_frames(t1 - 10, t3, 'running', 800, 2400))
			else:
				frames.extend(_gen_agenda_frames(t1, t2, 'failed', 200, 400))
				frames.extend(_gen_agenda_frames(t2, t3, 'complete', 800, 2400))
			if randint(0, 1):
				frames.extend(_gen_agenda_frames(t3, t4, 'pending', 0, 5))
			else:
				frames.extend(_gen_agenda_frames(t3, t4, 'running', 100, 400))

			job['agenda'] = frames

	if subjobs:
		for i, job in enumerate(jobs):
			subjobs = []
			semi = semi_random_list(0, 50)
			for i, n in enumerate(range(0, 50)):
				some_int = semi[i]
				subjob = {}
				subjob['name'] = 'subjob_' + str(n).zfill(3)
				subjob['id'] = 123111 + (i * 100) + n
				time_ = BASE_TIME + (i * 5) + (n * 5) - some_int
				subjob['timestart'] = time_
				subjob['timecomplete'] = time_ + some_int
				subjob['status'] = 'complete'
				subjobs.append(subjob)
			job['subjobs'] = subjobs

	if filters:
		pgrp = filters['pgrp']
		output = []
		for job in jobs:
			if job['pgrp'] == pgrp:
				output.append(job)
		jobs = output

	return jobs

def _hostinfo(filters={}, subjobs=False, fields=None, id=None, state=None, name=None):
	slots = ['16/16', '24/24', '32/32', '1/16', '5/16', '12/16',
			 '1/24', '17/24', '21/24', '15/32', '20/32', '30/32']
	down_slots = ['0/16', '0/24', '0/32']
	clusters = ['/atlas', '/hyperion', '/vancouver', '/los_angeles']
	states = ['active', 'idle', 'locked', 'down', 'random']

	hosts = []
	names = ['render_node_' + str(x).zfill(4) for x in range(1, 101)]


	for i in range(0, 5):
		host = {}
		host['name'] = names[0]
		host['cluster'] = clusters[0]

		host['state'] = 'active'
		host['resources'] = 'host.processors=' + str(slots[0])

		subjob = {}
		subjob['pid'] = JOBIDS[i][:-2]
		subjob['id'] = i

		host['subjobs'] = [subjob]
		hosts.append(host)

	for i, name in enumerate(names[1:16]):

		host = {}
		host['name'] = names[i]
		host['cluster'] = clusters[randint(0, 3)]
		host['state'] = 'active'
		host['resources'] = 'host.processors=' + str(slots[randint(0, 10)])

		subjob = {}
		subjob['pid'] = JOBIDS[i][:-2]
		subjob['id'] = randint(0, 101)
		host['subjobs'] = [subjob]
		hosts.append(host)

	for name in names[16:]:
		host = {}
		host['name'] = name
		host['cluster'] = clusters[randint(0, 3)]

		subjob = {}
		state = states[randint(0, 4)]
		if state is 'active':
			host['state'] = 'active'
			pid = randint(0, len(JOBIDS) - 1)
			pid = JOBIDS[pid][:-2]
			subjob['pid'] = pid
			host['resources'] = 'host.processors=' + str(slots[randint(0, 10)])

		if state is 'idle':
			host['state'] = 'active'
			subjob['pid'] = 0
			host['resources'] = 'host.processors=' + str(slots[randint(0, 10)])

		elif state is 'locked':
			host['state'] = 'active'
			subjob['pid'] = None
			host['resources'] = 'host.processors=' + '0/0'

		elif state is 'down':
			host['state'] = 'down'
			subjob['pid'] = 0
			host['resources'] = 'host.processors=' + str(down_slots[randint(0, 2)])

		else:
			host['state'] = 'active'
			pid = randint(123000, 124000)
			subjob['pid'] = pid
			host['resources'] = 'host.processors=' + str(slots[randint(0, 10)])

		subjob['id'] = randint(0, 101)
		host['subjobs'] = [subjob]
		hosts.append(host)

	return hosts

def stdout(subjobid, *extraSubjobids):
	subjobid = str(subjobid)
	found = re.search('^\d+$', subjobid)
	if found:
		subjobid += '.0'
	jobs = [subjobid]
	jobs.extend(extraSubjobids)
	jobs = map(str, jobs)
	jobs = [re.sub('\.', '_', x) for x in jobs]
	for job in jobs:
		found = re.search('^\d+$', job)
		if found:
			job = job + '_0'
	output = []
	for job in jobs:
		item = {}
		item['jobid'] = job[:-2]
		item['subid'] = job[-1:]
		item['data'] = ''
		if job in JOBIDS:
			f = os.path.join(LOGPATH, job + '.err')
			with open(f, 'r') as log:
				item['data'] = log.read()
		output.append(item)
	return output

def stdout(subjobid, *extraSubjobids):
	subjobid = str(subjobid)
	found = re.search('^\d+$', subjobid)
	if found:
		subjobid += '.0'
	jobs = [subjobid]
	jobs.extend(extraSubjobids)
	jobs = map(str, jobs)
	jobs = [re.sub('\.', '_', x) for x in jobs]
	for job in jobs:
		found = re.search('^\d+$', job)
		if found:
			job = job + '_0'
	output = []
	for job in jobs:
		item = {}
		item['jobid'] = job[:-2]
		item['subid'] = job[-1:]
		item['data'] = ''
		if job in JOBIDS:
			f = os.path.join(LOGPATH, job + '.out')
			with open(f, 'r') as log:
				item['data'] = log.read()
		output.append(item)
	return output

def set_priority(job_ids, priority):
	for job_id in job_ids:
		print job_id, 'set to', priority

JOBINFO = _jobinfo(filters={}, agenda=True, subjobs=True, fields=None, callbacks=True, id=None, status=None)
HOSTINFO = _hostinfo(filters={}, subjobs=True, fields=None, id=None, state=None, name=None)

def jobinfo(filters={}, agenda=False, subjobs=False, fields=None, callbacks=False, id=None, status=None):
	return JOBINFO

def hostinfo(filters={}, subjobs=False, fields=None, id=None, state=None, name=None):
	return HOSTINFO
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['setsupervisor', 'jobinfo', 'set_priority', 'stderr', 'stdout']

if __name__ == '__main__':
	main()
