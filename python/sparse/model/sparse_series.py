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
.. module:: sparse_series
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Special subclass of pandas Series for sparse data aggregation

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
from sparse.model.sparse_dataframe import SparseDataFrame
# ------------------------------------------------------------------------------

class SparseSeries(Series, Base):
	def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, name=None):
		super(SparseSeries, self).__init__(data=None, index=None, columns=None, dtype=None, copy=False)
		self._cls = 'SparseSeries'

		if type(data) is Series:
			self._data = data
		else:
			self._data = DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
	# --------------------------------------------------------------------------

	def to_type(self, dtype, inplace=False):
		data = self.data.apply(lambda x: to_type(x, dtype))

		if inplace:
			self.data = data
		return data

	def is_iterable(self, inplace=False):
		data = self.data.apply(lambda x: is_iterable(x))

		if inplace:
			self.data = data
		return data

	def make_iterable(self, inplace=False):
		data = self.data.apply(lambda x: make_iterable(x))

		if inplace:
			self.data = data
		return data

	def coerce_nulls(self, inpace=True):
		nulls = [None, '', [], {}, (), set(), OrderedDict()]
		def _coerce_nulls(item):
			if item in nulls:
				return numpy.nan
			else:
				return item
		data = self.apply(lambda x: _coerce_nulls(x))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def regex_match(self, pattern, group=0, ignore_case=False, inplace=False):
		data = self.data.apply(lambda x: regex_match(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_search(self, pattern, group=0, ignore_case=False, inplace=False):
		data = self.data.apply(lambda x: regex_search(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_sub(self, pattern, repl, group=0, count=0, ignore_case=False, inplace=False):
		data = self.data.apply(lambda x: regex_sub(pattern, repl, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_split(self, pattern, ignore_case=False, inplace=False):
		data = self.data.apply(lambda x: regex_split(pattern, x, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def remove_null_lines(self, inplace=True):
		mask = self[self == '']
		data = self
		data[mask] = numpy.nan
		data = data.dropna()
		data.reset_index(drop=True, inplace=True)

		if inplace:
			self.data = data
		return data

	def nan_to_bottom(self, inplace=False):
		data = self.data.dropna()
		buf = [numpy.nan] * (self.data.size - data.size)
		data = data.append(Series(buf))
		data = SparseSeries(list(data), index=self.data.index)

		if inplace:
			self.data = data
		return data

	def invert(self, inplace=True):
		patterns = sorted(list(set(self)))
		inversion_map = dict(zip(patterns, [x for x in reversed(patterns)]))
		data = self.data.apply(lambda x: inversion_map[x])

		if inplace:
			self.data = data
		return data

	def reduce_units(self, new_unit='-', min=0, inplace=True):
	    old = sort(self.data.unique())
	    new = range(0, len(old))
	    new = [new_unit * (x + min) for x in new]
	    lut = dict(zip(old, new))
	    data = self.data.apply(lambda x: lut[x])

	    if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def read_file(self, filepath, cleanup=True):
	    data = None
	    with open(filepath, 'r') as file_:
	        data = SparseSeries(file_.readlines(), name=filepath)
	    if cleanup:
	        data = data.str.strip()
	        data.remove_null_lines(inplace=True)
	    self.data = data
	    return data

	def to_SparseDataFrame(self, flatten=True):
		data = SparseDataFrame(self)
		if flatten:
			data.flatten(dtype=list, inplace=True)
		return data
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseSeries']

if __name__ == '__main__':
	main()