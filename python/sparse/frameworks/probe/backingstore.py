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

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import pandas
from pandas import Series
import numpy
from sparse.core.sparse_dataframe import SparseDataFrame
from sparse.utilities.utils import *
from sparse.utilities.errors import *
# ------------------------------------------------------------------------------

class BackingStore(Base):

	def __init__(self, name=None):
		super(BackingStore, self).__init__(name=name)
		self._cls = 'BackingStore'

		self._data = None
		self._results = None
		self._instruction_map = {}

	@property
	def data(self):
		return self._data

	@property
	def results(self):
		return self._results

	def get_database(self):
		self.update()
		database = {}
		database['dtype'] = 'json'
		database['data'] = self.data.to_json(orient='records')
		return database

	def _do_instructions(self, instructions):
		for instr in instructions:
			if instr not in self._instruction_map:
				raise KeyError('Instruction not defined in backingstore instruction map')

		for instr, args, kwargs in instructions.iteritems():
			self._instruction_map[instr](*args, **kwargs)

	def process_order(self, order):
		if order['dtype'] is 'json':
			self._results = pandas.read_json(order['data'], orient='records')
		elif order['dtype'] is 'DataFrame':
			self._results = results
		else:
			raise TypeError('Unrecognized data type')

		self._do_instructions(order['instructions'])

	@property
	def source_data(self):
		# method for retrieving source data
		raise EmptyFunction('Please define this function in your subclass')

	def update(self):
		data = []
		for datum in self.source_data:
			data.append(Series(datum))
		sdata = SparseDataFrame(data=data)
		sdata.flatten(inplace=True)
		sdata.coerce_nulls(inplace=True)
		data = sdata.data
		# data.dropna(how='all', axis=1, inplace=True)
		data['probe_id'] = data.index
		self._data = data
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