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
import json
import pandas
from sparse.utilities.errors import *
from sparse.utilities.utils import *
from sparse.core.spql_interpreter import SpQLInterpreter
from sparse.core.sparse_dataframe import SparseDataFrame
from sparse.frameworks.tune.tuner import Tuner
TUNER = Tuner()
# ------------------------------------------------------------------------------

class ProbeAPI(Base):
	def __init__(self, backingstore, updates='automatic', user_mode='automatic', name=None):
		super(ProbeAPI, self).__init__(name=name)
		self._cls = 'ProbeAPI'

		self._backingstore = backingstore
		self._updates = updates
		self._user_mode = user_mode
		self._database = None
		self._results = None
		self._spql = SpQLInterpreter()
		self._mongodb = None
		self._elasticsearch = None

	@property
	def database(self):
		if self._updates is 'automatic':
			self.update()
		return self._database

	@property
	def data(self):
		return self.database['data']

	@property
	def metadata(self):
		return self._database['metadata']

	@property
	def data_type(self):
		return self._database['metadata']['data_type']

	@property
	def results(self):
		return self._results

	def update(self):
		self._database = self._backingstore.get_database()

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

	def spql_search(self, string, data_type='sparse', field_operator='==', display_fields=[]):
		if string in TUNER['probe_api']['custom_search_words'].keys():
			string = TUNER['probe_api']['custom_search_words'][string]
			print 'SpQL search:', string

		if data_type == 'sparse':
			results = SparseDataFrame()
			results.read_json(self.data)
			results.spql_search(string, field_operator=field_operator, inplace=True)

			if len(results.data) == 0:
				raise NotFound('No search results found')
				
			if display_fields:
				results.data = results.data[display_fields]
				
			results = results.to_json(orient='records')
			self._results = results
					
		elif data_type is 'mongodb':
			query = self._spql.mongo_query
			self._results = self._mongodb.aggregate(query)
		
		elif data_type is 'elasticsearch':
			query = self._spql.elasticsearch_query
			self._results = self._elasticsearch.aggregate(query)

		else:
			raise TypeError('Database type not recognized')
	# --------------------------------------------------------------------------

	def _send_order(self, instructions):
		order = {}
		order['data'] = self._results
		order['metadata'] = self.metadata
		order['instructions'] = instructions
		order = json.dumps(order)

		self._backingstore.process_order(order)
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