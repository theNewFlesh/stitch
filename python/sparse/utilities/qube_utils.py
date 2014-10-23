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
.. module:: qube_utils
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Qube BackingStore utilities

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import re
from copy import copy
import time
import numpy
from pandas import DataFrame
import pandas
from sparse.utilities.utils import *
# ------------------------------------------------------------------------------

def flatten_qube_field(database, fields):   
	new_db = []
	for job in database:       
		frames = []
		for field in fields:
			for item in job[field]:
				if type(item) != dict:
					item = eval(str(item))
				temp = {}
			for key, value in item.iteritems():
				temp[field + '_' + key] = value    
			frames.append(temp)

		head = job
		
		for frame in frames:
			new_job = copy(head)
			for key, value in frame.iteritems():
				new_job[key] = value
			new_db.append(new_job)
			
	return new_db

def fix_missing_fields(database, fields):
	for field in fields:
		keys = []
		for job in database:
			try:
				job[field] = job[field]
			except:
				continue
			for items in job[field]:
				if items.__class__.__name__ != 'dict':
					items = eval(str(items))
				for key in items:
					keys.append(key)
		
		replacement = {}
		for key in keys:
			replacement[key] = None
		replacement = [replacement]

		for job in database:
			if not field in job.keys():
				job[field] = replacement

	return database

def get_slots(item):
	slots_re = re.compile('host\.processors=(\d+)/(\d+)')
	found = slots_re.search(item)
	if found:
		return {'used': int(found.group(1)), 'total': int(found.group(2))}
	else:
		return {'used': None, 'total': None}

def str_to_nan(item):
	if item == u'' or item == '':
		return ''
	else:
		return item

def get_procs(item):
	procs_re = re.compile('processors=(\d+)')
	found = procs_re.search(item)
	if found:
		return int(found.group(1))
	else:
		return ''

def get_plus_procs(item):
	procs_re = re.compile('processors=\d+(.)')
	found = procs_re.search(item)
	if found:
		if found.group(1) == '+':
			return '+'
	return ''

def get_ram(item):
	ram_re = re.compile('memory=(\d+)')
	found = ram_re.search(item)
	if found:
		return int(found.group(1))
	else:
		return ''

def get_plus_ram(item):
	pram_re = re.compile('memory=\d+(.)')
	found = pram_re.search(item)
	if found:
		if found.group(1) == '+':
			return '+'
	return ''

def get_jobtype(item):
	job_re = re.compile('maya|mayabatch|nuke|vray|vrscene|houdini|generate|email|shotgun', 
						flags=re.IGNORECASE)
	found = job_re.search(item)
	if found:
		return found.group(0)
	else:
		return numpy.nan

def get_agenda_stats(item, id, embed_graphs=False):
	data = DataFrame(item)
	rounding = 2
	epoch = 946800000
	
	# Coerce bad timestamps
	data['timestart'] = data['timestart'].apply(lambda x: numpy.nan if x < epoch else x)

	# Add framespan and failedframes
	mask = data[data['status'] == 'running']
	data.loc[mask.index, 'lastupdate'] = time.time()
	data['span'] = (data['lastupdate'] - data['timestart']) / 3600

	output = {}
	mask = data[data['span'] > (0.5 / 60)] # Drop frames under 30 sec
	output['frame_max']              = round_to(mask['span'].max(), rounding)
	output['frame_min']              = round_to(mask['span'].min(), rounding) 
	output['frame_sum']              = round_to(mask['span'].sum(), rounding)
	output['frame_retry']            = round_to(mask['retry'].max(), rounding)
	output['frame_total']            = len(data)
	output['frame_complete_total']   = len(mask[mask['status'] == 'complete'])
	frmcomp = output['frame_complete_total'] / output['frame_total']
	output['frame_complete_percent'] = round_to(frmcomp, 3)

	c = len(mask)
	s = output['frame_sum'] - (output['frame_min'] * c)
	f = (output['frame_max'] - output['frame_min']) * c
	d = numpy.nan
	try:
		d = s/f
		d = round_to(d, 3)
	except ZeroDivisionError:
		pass
	output['frame_distribution'] = d

	failed = data[data['status'] == 'failed']
	output['failed_frames_total'] = len(failed)
	output['failed_frame_names'] = ', '.join(failed['name'].tolist())
	output['failed_frame_hosts'] = ', '.join(failed['host'].tolist())
	output['host_total']         = data['host'].nunique()
	output['pid_total']          = data['pid'].nunique()
	output['status_total']       = data['status'].nunique()
	output['subid_total']        = data['subid'].nunique()
	output['name_total']         = data['name'].nunique()
	
	output['count_max']          = data['count'].max()
	output['id_max']             = data['id'].max()
	output['retry_max']          = data['retry'].max()
	output['retrydelay_max']     = data['retrydelay'].max()
	output['timecomplete_max']   = data['timecomplete'].max()
	output['timecumulative_max'] = data['timecumulative'].max()

	output['timestart_min']      = data['timestart'].min()

	output['subids'] 	         = ', '.join(data['subid'].apply(str).unique().tolist())

	subids = failed['subid'].apply(str).tolist()
	output['failed_subids'] 	 = ', '.join(subids)

	if embed_graphs:
		fg = data[['span']].copy()
		fg.columns = ['all']
		
		fg['failed'] = fg['all']
		mask = data[data['status'] != 'failed']
		fg.loc[mask.index, 'failed'] = numpy.nan

		fg['complete'] = fg['all']
		mask = data[data['status'] != 'complete']
		fg.loc[mask.index, 'complete'] = numpy.nan

		fg['running'] = fg['all']
		mask = data[data['status'] != 'running']
		fg.loc[mask.index, 'running'] = numpy.nan

		fg = fg.sort(columns=['all'], ascending=False)
		fg.dropna(how='all', inplace=True)
		fg.reset_index(drop=True, inplace=True)

		fg2 = fg.copy()[['all']]
		fg2.columns = [[id], ['all']]
		output['frame_graph'] = fg2.to_dict()

		fg.columns = [[id] * 4, ['all', 'failed', 'complete', 'running']]
		output['frame_graph_detailed'] = fg.to_dict()

	return output

def create_complete_subids(id, subids):
	if id:
		if subids:
			return [str(id) + '.' + str(x) for x in subids.split(', ')]
		return [str(id) + '.0']
	return numpy.nan
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = [ 'flatten_qube_field', 'fix_missing_fields', 'get_slots', 'str_to_nan',
			'get_procs', 'get_plus_procs', 'get_ram', 'get_plus_ram', 'get_jobtype',
			'get_agenda_stats', 'create_complete_subids']

if __name__ == '__main__':
	main()