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

import warnings
import re
import json
import time
from copy import copy
from datetime import datetime
import numpy
import pandas
from pandas import DataFrame, Series

from sparse.core.sparse_dataframe import SparseDataFrame
from sparse.frameworks.probe.backingstore import BackingStore
from sparse.utilities.qube_utils import *
from sparse.utilities.utils import *
from sparse.utilities.errors import *
import sparse.utilities.mock.qb as qb
from sparse.frameworks.tune.tuner import Tuner
TUNER = Tuner()
# ------------------------------------------------------------------------------

class QubeBackingStore(BackingStore):
	def __init__(self, 
						jobinfo=False,
						hostinfo=False,
						group=False,
						supervisor=None,
						agenda=False,
						callbacks=False,
						fields=[],
						filters={},
						id=None,
						name=None,
						state=None,
						status=None,
						subjobs=False,
						embed_graphs=False):

		super(QubeBackingStore, self).__init__(name=name)
		self._cls = 'QubeBackingStore'

		self._jobinfo = jobinfo
		self._hostinfo = hostinfo
		self._group = group
		self._supervisor = supervisor
		self._agenda = agenda
		self._callbacks = callbacks
		self._fields = fields
		self._filters = filters
		self._id = id
		self._name = name
		self._state = state
		self._status = status
		self._subjobs = subjobs
		self._embed_graphs = embed_graphs
		self._database = qb
		self._data = None
		self._results = None
		self._instruction_map = {}

		self._database.setsupervisor(self._supervisor)
	# --------------------------------------------------------------------------
	def _get_agenda_stat(self, item):
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

		mask = data[data['status'] == 'failed']
		output['failed_frame_names'] = ', '.join(mask['name'].tolist())
		output['failed_frame_hosts'] = ', '.join(mask['host'].tolist())
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

		if self._embed_graphs:
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
			fg2.columns = [data['pid'].head(1).tolist(), ['all']]
			output['frame_graph'] = fg2.to_dict()

			fg.columns = [data['pid'].head(1).tolist() * 4, 
							['all', 'failed', 'complete', 'running']]
			output['frame_graph_detailed'] = fg.to_dict()

		return output

	def _get_agenda_stats(self, data):
		sdata = SparseDataFrame(data)
		sdata.cross_map('id', 'agenda', lambda x: True, self._get_agenda_stat, inplace=True)
		data = sdata.flatten(prefix=True)
		return data
	# --------------------------------------------------------------------------

	def _job_update(self):
		data = pandas.read_json(self._job_data, orient='records')
		data = data.applymap(lambda x: {} if x is None else x)

		sdata = SparseDataFrame(data)
		sdata.flatten(inplace=True)
		data= sdata.data
		data = data.applymap(lambda x: str_to_nan(x))

		# Add custom fields
		data['procs'] = data['reservations'].apply(lambda x: get_procs(x))
		data['procs+'] = data['reservations'].apply(lambda x: get_plus_procs(x))
		data['ram'] = data['reservations'].apply(lambda x: get_ram(x))
		data['ram+'] = data['reservations'].apply(lambda x: get_plus_ram(x))
		data['failed_frame_total'] = data['todotally_failed']
		data['dependency'] = data['pgrp']
		mask = data['dependency'] - data['id']
		data['dependency'][mask == 0] = None
		data['percent_done'] = data['todotally_complete'] / data['todo']
		data['percent_done'] = data['percent_done'].apply(lambda x: round_to(x, 3) * 100)

		if self._agenda:
			data = self._get_agenda_stats(data)

		data.reset_index(drop=True, inplace=True)
		data['probe_id'] = data.index

		data.columns = TUNER.tune(data.columns, 'qube_backingstore')
		data = SparseDataFrame(data)

		self._data = data

	@property
	def _job_data(self):
		jobs = self._database.jobinfo(  fields=self._fields,
										filters=self._filters,
										id=self._id,
										status=self._status,
										agenda=self._agenda,
										subjobs=self._subjobs,
										callbacks=self._callbacks)
		jobs = json.dumps([dict(job) for job in jobs])
		return jobs
	# --------------------------------------------------------------------------

	def _append_custom_host_subjob_fields(self, data):
		mask = data[data['slots_total'] ==0]
		data.loc[mask.index, 'state'] = 'locked'
		return data

	def _group_host_data(self, data):
		cols = ['count', 'data', 'host', 'id', 'lastupdate', 'pid', 'result',
				'retry', 'status', 'timecomplete', 'timestart']
		cols = ['subjobs_' + x for x in cols]
		cols_ = cols
		cols_.insert(0, 'name')

		temp = data[cols_]
		temp = temp.groupby('name', as_index=False).agg(lambda x: x.nunique())
		data = data.groupby('name', as_index=False).first()
		data[cols] = temp[cols]

		mask = data[data['state'] == 'active']
		mask = mask[mask['subjobs_pid'] == 0]
		data.loc[mask.index, 'state'] = 'idle'

		return data
	# --------------------------------------------------------------------------

	def _host_update(self):
		data = json.loads(self._host_data)

		fields = []
		if self._subjobs:
			fields.append('subjobs')
		if fields:
			data = fix_missing_fields(data, fields)
			data = flatten_qube_field(data, fields)

		data = DataFrame(data)
		data = data.applymap(lambda x: numpy.nan if x is {} else x)
		data['slots'] = data['resources'].apply(lambda x: get_slots(x))

		sdata = SparseDataFrame(data)
		sdata.flatten(inplace=True)
		data = sdata.data

		data['slot_percent'] = data['slots_used'] / data['slots_total']
		data['slot_percent'] = data['slot_percent'].apply(lambda x: round_to(x, 3) * 100)

		if self._subjobs:
			data = self._append_custom_host_subjob_fields(data)

		if self._group:
			data = self._group_host_data(data)

		data.reset_index(drop=True, inplace=True)
		data['probe_id'] = data.index
		sdata.data = data

		self._data = sdata

	@property
	def _host_data(self):
		hosts = self._database.hostinfo( fields=self._fields,
										filters=self._filters,
										name=self._name,
										state=self._state,
										subjobs=self._subjobs)
		hosts = json.dumps([dict(host) for host in hosts])
		return hosts

	@property
	def supervisor(self):
		return self._supervisor
	# --------------------------------------------------------------------------

	@property
	def source_data(self):
		if self._jobinfo:
			return self._job_data
		elif self._hostinfo:
			return self._host_data
		else:
			raise NotFound('Database not specified')

	def update(self):
		if self._jobinfo:
			self._job_update()
		elif self._hostinfo:
			self._host_update()
		else:
			raise NotFound('Database not specified')
	# --------------------------------------------------------------------------

	def set_priority(self, priority): 
		job_ids = self._results.data['id'].tolist()
		self._database.set_priority(job_ids, priority)
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