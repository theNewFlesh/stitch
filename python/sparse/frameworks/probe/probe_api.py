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
.. module:: probe_api
	:date: 04.13.2014
	:platform: Unix
	:synopsis: API for Probe BackingStore

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
from collections import OrderedDict
import pandas
pandas.options.display.expand_frame_repr= False
from sparse.utilities.utils import *
from sparse.core.spql_interpreter import SpQLInterpreter
# ------------------------------------------------------------------------------

class ProbeAPI(Base):
	def __init__(self, backingstore, updates='automatic', user_mode='automatic', name=None):
		super(ProbeAPI, self).__init__(name=name)
		self._cls = 'ProbeAPI'

		self._backingstore = backingstore
		self._updates = updates
		self._user_mode = user_mode
		self._database = self._backingstore.get_database()
		self._data = self._database['data']
		self._datatype = self._database['dtype']
		self._results = None
		self._instructions = OrderedDict()
		self._spql = SpQLInterpreter()
		self._mongodb = None
		self._elasticsearch = None

	@property
	def database(self):
		if self._updates is 'automatic':
			self._database = self._backingstore.get_database()
		return self._database

	@property
	def data(self):
		return self.database['data']

	@property
	def datatype(self):
		return self.database['dtype']

	@property
	def results(self):
		return self._results

	@property
	def instructions(self):
		return self._instructions

	def clear_instructions(self):
		self._instructions = OrderedDict()

	def issue_order(self):
		self._backingstore.process_order(self.order)
	# --------------------------------------------------------------------------

	@property
	def user_mode(self):
		return self._user_mode

	def manual_mode(self):
		self._user_mode = 'manual'

	def automatic_mode(self):
		self._user_mode = 'automatic'
	# --------------------------------------------------------------------------

	@property
	def updates(self):
		return self._updates

	def manual_updates(self):
		self._updates = 'manual'

	def automatic_updates(self):
		self._updates = 'automatic'
	# --------------------------------------------------------------------------

	@property
	def elasticsearch(self):
		return self._elasticsearch

	@property
	def mongodb(self):
		return self._mongodb

	def spql_search(self, string, database='dataframe', field_operator='==', display_fields=[]):
		self._spql.search(string)
		if database is 'mongodb':
			query = self._spql.mongo_query
			self._results = self._mongodb.aggregate(query)
		elif database is 'elasticsearch':
			query = self._spql.elasticsearch_query
			self._results = self._elasticsearch.aggregate(query)
		else:
			if self.datatype is 'json':
				data = pandas.read_json(self.data, orient='records')
				results = self._spql.dataframe_query(data)
				if display_fields:
					results = results[display_fields]
				results = results.to_json(orient='records')
				self._results = results
			elif self.datatype is 'dataframe':
				results = self._spql.dataframe_query(data)
				if display_fields:
					results = results[display_fields]
				self._results = results
			else:
				raise TypeError('Database data type not recognized')
	# --------------------------------------------------------------------------

	def _do(self, instructions):
		if self._user_mode is 'automatic':
			self.clear_instructions()
			self._instructions = instructions
			self.issue_order(self.order)
		else:
			for key, val in instructions.iteritems():
				self._instructions[key] = val

	def update_view(self):
		self._do({'update_view': True})

	def set_priority(self, integer):
		self._do({'set_priority': integer})

	def set_instances(self, integer):
		self._do({'set_instances': integer})

	def adjust_priority(self, integer):
		self._do({'adjust_priority': integer})

	def adjust_instances(self, integer):
		self._do({'adjust_instances': integer})
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['ProbeAPI']

if __name__ == '__main__':
	main()