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
.. module:: backingstore
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Backingstore base for interfacing with Probe API

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''
# ------------------------------------------------------------------------------

import json
import pandas
from pandas import Series
import numpy
from sparse.core.sparse_dataframe import SparseDataFrame
from sparse.core.utils import *
from sparse.core.errors import *
# ------------------------------------------------------------------------------

class BackingStore(Base):
	def __init__(self, name=None):
		super(BackingStore, self).__init__(name=name)
		self._cls = 'BackingStore'
		self._data = None
		self._results = None

	@property
	def source_data(self):
		# method for retrieving source data
		raise EmptyFunction('Please define this function in your subclass')

	@property
	def data(self):
		return self._data

	@property
	def results(self):
		return self._results
	# --------------------------------------------------------------------------

	def get_database(self):
		self.update()
		database = {}
		database['metadata'] = {}
		database['metadata']['data_type'] = 'json orient=records, DataFrame'
		database['data'] = self._data.to_json(orient='records')
		return database

	def update(self):
		data = []
		for datum in self.source_data:
			data.append(Series(datum))
		sdata = SparseDataFrame(data)
		sdata.flatten()
		sdata.coerce_nulls()
		data = sdata._data
		# data.dropna(how='all', axis=1, inplace=True)
		data['probe_id'] = data.index
		sdata._data = data
		self._data = sdata
	# --------------------------------------------------------------------------

	def _execute_instruction(self, instruction):
		func = instruction['func']
		args = instruction['args']
		kwargs = instruction['kwargs']
		getattr(self, func)(*args, **kwargs)

	def process_order(self, order):
		order = json.loads(order)
		dtype = order['metadata']['data_type']
		if dtype == u'json orient=records, DataFrame':
			results = SparseDataFrame()
			results.read_json(order['data'], orient='records')
			self._results = results
		else:
			raise TypeError('Unrecognized data type: ' + dtype)

		for instruction in order['instructions']:
			self._execute_instruction(instruction)
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['BackingStore']

if __name__ == '__main__':
	main()
