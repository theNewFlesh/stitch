#! /usr/bin/env python
# Alex Braun 04.23.2014

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
.. module:: sparse_dataframe
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Special subclass of pandas DataFrame for sparse data aggregation

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
import re
from collections import OrderedDict
import pandas
from pandas import DataFrame, Series
import numpy
from sparse.utilities.utils import *
from sparse.core.spql_parser import SpQLParser
# ------------------------------------------------------------------------------

class SparseDataFrame(Base):
	def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, name=None):
		super(SparseDataFrame, self).__init__(name=name)
		self._cls = 'SparseDataFrame'

		if type(data) is DataFrame:
			self.data = data
		else:
			self.data = DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
		super(SparseDataFrame, self).__init__(name=name)
		self._cls = 'SparseDataFrame'
	# --------------------------------------------------------------------------

	def to_type(self, dtype, inplace=False):
		data = self.data.applymap(lambda x: to_type(x, dtype))

		if inplace:
			self.data = data
		return data

	def is_iterable(self, inplace=False):
		data = self.data.applymap(lambda x: is_iterable(x))

		if inplace:
			self.data = data
		return data

	def make_iterable(self, inplace=False):
		data = self.data.applymap(lambda x: make_iterable(x))

		if inplace:
			self.data = data
		return data

	def coerce_nulls(self, inplace=True):
		nulls = [None, '', [], {}, (), set(), OrderedDict()]
		def _coerce_nulls(item):
			if item in nulls:
				return numpy.nan
			else:
				return item
		data = self.data.applymap(lambda x: _coerce_nulls(x))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def regex_match(self, pattern, group=0, ignore_case=False, inplace=False):
		data = self.data.applymap(lambda x: regex_match(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_search(self, pattern, group=0, ignore_case=False, inplace=False):
		data = self.data.applymap(lambda x: regex_search(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_sub(self, pattern, repl, group=0, count=0, ignore_case=False, inplace=False):
		data = self.data.applymap(lambda x: regex_sub(pattern, repl, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_split(self, pattern, ignore_case=False, inplace=False):
		data = self.data.applymap(lambda x: regex_split(pattern, x, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def flatten(self, dtype=dict, prefix=True, inplace=False):
		mask = self.data.applymap(lambda x: bool_test(type(x), '==', dtype))
		iterables = self.data[mask]
		iterables = iterables.dropna(how='all', axis=1)

		new_data = self.data.drop(iterables.columns, axis=1)
		frames = [new_data]
		for col in iterables.columns:
			frame = DataFrame(self.data[col].tolist())
			if prefix:
				columns = {}
				for k in frame.columns:
					columns[k] = str(col) + '_' + str(k)
				frame.rename(columns=columns)
			frames.append(frame)
		data = pandas.concat(frames, axis=1)

		if inplace:
			self.data = data
		return data

	def drop_by_mask(self, mask, how='all', axis=0, inplace=False):
		mask = self.data[mask]
		mask = mask.dropna(how=how, axis=axis)
		data = None
		if axis == 0:
			data = self.data.ix[mask.index]
		if axis == 1:
			data = self.data[mask.columns]
		data.reset_index(drop=True, inplace=True)

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def spql_search(self, string, field_operator='re', inplace=False):
		spql = SpQLInterpreter()
		spql.search(string)
		data = spql.dataframe_query(self.data, field_operator=field_operator)

		if inplace:
			self.data = data
		return data
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseDataFrame']

if __name__ == '__main__':
	main()
