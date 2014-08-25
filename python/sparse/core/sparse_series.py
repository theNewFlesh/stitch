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

'''The sparse_series module contains the SparseSeries class.

The SparseSeries class is an extension of the Series class from the Pandas
library.  SparseSeries contain additional methods for converting sparse data,
such as esoteric databases, logs and custom made tables, into SparseSeries,
with the actual data residing within a Pandas Series.  

Date:
	08.24.2014

Platform:
	Unix

Author:
	Alex Braun <ABraunCCS@gmail.com> <http://www.AlexBraunVFX.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
import re
from collections import OrderedDict
import pandas
from pandas import DataFrame, Series
import numpy
from sparse.utilities.utils import *
from sparse.core.sparse_dataframe import SparseDataFrame
# ------------------------------------------------------------------------------

class SparseSeries(Base):
	'''Class for converting sparse data into well-formated, tabular data

	The SparseSeries class which is an extension of the Series class from
	the Pandas library.  SparseSeries contain additional methods for converting
	sparse data, such as esoteric databases, logs and custom made tables, into
	SparseSeries, with the actual data residing	within a Pandas Series,
	acessible through the data attribute.

	Attributes:
		cls (str): Class descriptor
		name (str): Name descriptor
		data (Series): Internal Series where data is actually stored
	'''

	def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, name=None):
		'''SparseSeries initializer

		Args:
			data (array-like or Series, optional): Data to be ingested. Default: None.
			index (Index or array-like, optional): Index of Series. Default: None.
			columns (Index or array-like, optional): Column names. Default: None.
			dtype (dtype, optional): Data type to force, otherwise infer. Default: None.
			copy (bool, optional): Copy data from inputs. Default: None.
			name (str, optional): Name of object. Default: None.
		'''

		super(SparseSeries, self).__init__(name=name)
		self._cls = 'SparseSeries'

		if type(data) is Series:
			self._data = data
		else:
			self._data = DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
	# --------------------------------------------------------------------------

	def to_type(self, dtype, inplace=False):
		'''Converts data to specified type, leaving uncoercible types alone

		Args:
			dtype (type): Type to convert data into.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series.
		'''
		data = self.data.apply(lambda x: to_type(x, dtype))

		if inplace:
			self.data = data
		return data

	def is_iterable(self, inplace=False):
		'''Returns a boolean mask indicating which elements are iterable

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns: 
			Series mask.
		'''
		data = self.data.apply(lambda x: is_iterable(x))

		if inplace:
			self.data = data
		return data

	def make_iterable(self, inplace=False):
		'''Makes all the elements iterable

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of iterable elements.
		'''

		data = self.data.apply(lambda x: make_iterable(x))

		if inplace:
			self.data = data
		return data

	def coerce_nulls(self, inpace=False):
		'''Coerce all null elements into numpy.nan

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of coerced elements.
		'''

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
		'''Apply regular expression matches to all Series elements

		Args:
			pattern (str): Regular expression pattern to match.
			group (int, optional): Regular expression group to return. Default: 0.
			ignore_case (bool, optional): Ignore case. Default: False.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of regex matches.
		'''

		data = self.data.apply(lambda x: regex_match(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_search(self, pattern, group=0, ignore_case=False, inplace=False):
		'''Apply regular expression searches to all Series elements

		Args:
			pattern (str): Regular expression pattern to search.
			group (int, optional): Regular expression group to return. Default: 0.
			ignore_case (bool, optional): Ignore case. Default: False.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of regex searches.
		'''

		data = self.data.apply(lambda x: regex_search(pattern, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_sub(self, pattern, repl, group=0, count=0, ignore_case=False, inplace=False):
		'''Apply regular expression substitutions to all Series elements

		Args:
			pattern (str): Regular expression pattern to substitute.
			repl (str): String to replace pattern.
			group (int, optional): Regular expression group to return. Default: 0.
			count (int, optional): Maximum number of occurences to be replaced. Default: 0.
			ignore_case (bool, optional): Ignore case. Default: False.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of regex substitutions.
		'''

		data = self.data.apply(lambda x: regex_sub(pattern, repl, x, group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_split(self, pattern, ignore_case=False, inplace=False):
		'''Splits elements into list of found regular expression groups

		Args:
			pattern (str): Regular expression pattern with groups, ie. "(foo)(bar)".
			ignore_case (bool, optional): Ignore case. Default: False.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of with matched elements as lists of groups.
		'''

		data = self.data.apply(lambda x: regex_split(pattern, x, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def remove_null_lines(self, inplace=False):
		'''Removes null ('') elements from data

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series sans null elements.
		'''

		mask = self[self == '']
		data = self
		data[mask] = numpy.nan
		data = data.dropna()
		data.reset_index(drop=True, inplace=True)

		if inplace:
			self.data = data
		return data

	def nan_to_bottom(self, inplace=False):
		'''Pushes all nan elements to the bottom rows of the Series

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series with nan elements at the bottom.
		'''

		data = self.data.dropna()
		buf = [numpy.nan] * (self.data.size - data.size)
		data = data.append(Series(buf))
		data = SparseSeries(list(data), index=self.data.index)

		if inplace:
			self.data = data
		return data

	def invert(self, inplace=False):
		'''Inverts data according to its own unique values

		Invert creates a list of all unique data elements, pairs that list with
		its inverse and substitutes each element in the data with its 
		counterpart.

		Args:
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Inverted Series.

		Example:
			>>> print ss.data
				0    10
				1    20
				2    30
				3    40
				4    40

			>>> ss.invert(inplace=True)
			>>> print ss.data	
				0    40
				1    30
				2    20
				3    10
				4    10
		'''

		patterns = sorted(list(set(self.data)))
		inversion_map = dict(zip(patterns, [x for x in reversed(patterns)]))
		data = self.data.apply(lambda x: inversion_map[x])

		if inplace:
			self.data = data
		return data

	def reduce_units(self, new_unit='-', min=0, inplace=False):
		'''Replaces all units within a series with a set of smaller units

		Args:
			new_unit (str, optional): Replacement character. Default: '-'.
			min (int, optional): Minimum length of units. Default: 0.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series of reduced units.

		Example:
			Suppose you are trying to reduce the white space in a file's
			indentation, without disrupting its hierarchical structure. You
			first strip the indentation into its own series, and then reduce it.

			>>> file = file.readlines()
			>>> print file
				:A
					:B
						:C
					:E
						:F
			>>> indents = [x.split(':')[0] for x in file]
			>>> data = [x.split(':')[1] for x in file]
			>>> print indents
			['	', '		', '			', '		', '			']
			>>> ss = SparseSeries(indents)
			>>> ss.reduce_units(' ', inplace=True)
			>>> print ss
			['', ' ', '  ', ' ', '  ']
			>>> print [line for line in zip(indents, data)]
			A
			 B
			  C
			 E
			  F
		'''
		
		old = sorted(self.data.unique())
		new = range(0, len(old))
		new = [new_unit * (x + min) for x in new]
		lut = dict(zip(old, new))
		data = self.data.apply(lambda x: lut[x])

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def read_file(self, filepath, cleanup=True):
		'''Reads file into SparseSeries

		Args:
			filepath (str): Path to file.
			inplace (bool, optional): Apply changes in place. Default: False.

		Returns:
			Series with each file line as an element.
		'''
		
		data = None
		with open(filepath, 'r') as file_:
			data = SparseSeries(file_.readlines(), name=filepath)
		if cleanup:
			data = data.str.strip()
			data.remove_null_lines(inplace=True)
		self.data = data
		return data

	def to_SparseDataFrame(self, flatten=True):
		'''Converts data to a SparseDataFrame

		Args:
			flatten (bool, optional): Flattens data. Default: True.

		Returns:
			SparseDataFrame.
		'''

		data = SparseDataFrame(self.data)
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