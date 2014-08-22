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
from sparse.core.spql_interpreter import SpQLInterpreter
# ------------------------------------------------------------------------------

class SparseDataFrame(Base):
	def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, name=None):
		super(SparseDataFrame, self).__init__(name=name)
		self._cls = 'SparseDataFrame'
		self._spql = SpQLInterpreter()

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

	def coerce_nulls(self, inplace=False):
		nulls = [   None,      '',      [],      {},      (),      set(),      OrderedDict(),
				   [None],    [''],    [[]],    [{}],    [()],    [set()],    [OrderedDict()],
				   (None),    (''),    ([]),    ({}),    (()),    (set()),    (OrderedDict()),
						   set(''), set([]), set({}), set(()), set(set()), set(OrderedDict())   

		]
		def _coerce_nulls(item):
			if item in nulls:
				return numpy.nan
			else:
				return item
		data = self.data.applymap(lambda x: _coerce_nulls(x))

		if inplace:
			self.data = data
		return data

	def nan_to_bottom(self, inplace=True):
		def _nan_to_bottom(item):
			data = item.dropna()
			buf = [numpy.nan] * (item.size - data.size)
			data = data.append(Series(buf))
			data = Series(list(data), index=item.index)
			return data

		data = self.data.apply(lambda x: _nan_to_bottom(x))

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
				frame.rename(columns=columns, inplace=True)
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

	def stack_by_column(self, column, inplace=False):
		frames = []
		max_len = 0
		cols = list(self.data.columns.drop(column))
		grps = self.data.groupby(column)
		   
		for item in self.data[column].unique():
			group = grps.get_group(item)
			group = group[cols]
			group.columns = [[item] * len(cols), cols]
			frames.append(group)

			if len(group) > max_len:
				max_len = len(group)

		for frame in frames:
			bufr = frame.head(1)
			bufr = bufr.apply(lambda x: numpy.nan)

			buf_len = max_len - len(frame)
			for i in range(buf_len):
				bufr.append(bufr)
			frame.append(bufr, ignore_index=True)
			frame.reset_index(drop=True, inplace=True)

		data = pandas.concat(frames, axis=1)

		if inplace:
			self.data = data
		return data

	def unstripe(self, inplace=False):
		data = self.data.reset_index(level=1, drop=True)
		
		new_cols = data.columns.unique().tolist()
		cols = Series(data.columns)
		mask = cols.duplicated()
		bad_cols = cols[mask].tolist()
		good_cols = set(cols.unique()).difference(bad_cols)

		items = []
		for col in new_cols:
			if col in bad_cols:
				item = data[col].unstack()
				item = Series(item.values)
				items.append(item)
			else:
				items.append(data[col])
			
		data = irregular_concat(items, axis=1, ignore_index=False)
		data.columns = new_cols
		
		if inplace:
			self.data = data
		return data

	def unique(self, inplace=True):
		data = self.data
		mask = data.apply(lambda x: x.duplicated())
		data[mask] = numpy.nan

		if inplace:
			self.data = data
		return data

	def cross_map(self, source_column, target_column, mask_predicate, target_predicate=None, inplace=False):
    	data = self.data
    	mask = data[source_column].apply(mask_predicate)
		if target_predicate:
    		data[target_column][mask] = data[target_column][mask].apply(target_predicate)
		else:
			data[target_column][mask] = data[source_column][mask]

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------
	
	def read_nested_dict(self, item, name, inplace=False):
		values = flatten_nested_dict(item, name).values()
		index = nested_dict_to_index(item, name)
		data = DataFrame(values, index=index)
		mask = data.apply(lambda x: x != 'null')
		mask = mask[mask].dropna()
		data = data.ix[mask.index]

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------
	
	def spql_search(self, string, field_operator='==', inplace=False):
		data = self._spql.dataframe_query(self.data, field_operator=field_operator)

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
