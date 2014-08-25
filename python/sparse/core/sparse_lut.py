#! /usr/bin/env python
# Alex Braun 08.25.2014

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

'''The sparse_lut module contains the SparseLut class.

The SparseLut class is a lookup table used for converting non-numeric or
partially numeric data, into numeric data.  This data can then be graphed using
the Matplotlib library.

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
from itertools import *
import pandas
from pandas import DataFrame, Series, Panel
import numpy
from sparse.utilities.utils import *
from sparse.core.spql_parser import SpQLParser
from sparse.core.sparse_series import SparseSeries
from sparse.core.sparse_dataframe import SparseDataFrame
# ------------------------------------------------------------------------------

class SparseLUT(Base):
	'''Lookup table for non-numeric or partially numeric data

	Attributes:
		cls (str): Class descriptor.
		name (str): Name descriptor.
		keys (DataFrame): Input data that has been reduced to unique values.
		values (DataFrame): Numeric values mapped to the keys.
	'''

	def __init__(self, data, null='missing data', null_value=-1.0, name=None):
		'''SparseLut initializer

		Args:
			data (DataFrame): Keys data.
			null (string, optional): Placeholder for missing keys data. Default: 'missing data'.
			null_value (int, optional): Placeholder value for missing values data. Default: -1.0.
			name (string, optional): Name descriptor. Default: None.
		'''
		
		super(SparseLUT, self).__init__(name=name)
		self._cls = 'SparseLUT'

		self._null = null
		self._null_value = null_value
		self._keys = data
		self._reduce_keys()
		self._values = None
		self.generate_values()

	def _reduce_keys(self):
		'''Semi-private method for reducing the keys table'''

		data = SparseDataFrame(self._keys)
		data.unique(inplace=True)
		data.nan_to_bottom(inplace=True)
		data = data.data
		data.dropna(how='all', inplace=True)
		data[data.apply(pandas.isnull)] = self._null
		self._keys = data
	
	def generate_values(self, start=0, step=1):
		'''Generate a table of floating point values according to the keys table 

		Args:
			start (int, optional): Minimum floating point value. Default: 0.
			step (int, optional): Amount to step between values. Default 1.
		'''
		data = self._keys.copy()
		mask = data.applymap(lambda x: bool_test(x, '==', [self._null]))
		x, y = data.shape
		vals = cycle(range(start, x, step))
		data = data.applymap(lambda x: vals.next())
		data[mask] = self._null_value
		data = data.applymap(lambda x: float(x))
		self._values = data 
	# --------------------------------------------------------------------------

	@property
	def keys(self):
		'''Ketys table

		Returns:
			DataFrame of keys.
		'''
		return self._keys

	@property
	def values(self):
		'''Values table

		Returns:
			DataFrame of numeric values.
		'''

		return self._values
	# --------------------------------------------------------------------------

	def lookup(self, column, key):
		'''Lookup numeric value for given key with given column

		Args:
			column (column name): Name of column to look in.
			key (key): Key to search for within column.

		Returns:
			Numeric equivalent of key. 
		'''

		keys = self._keys[column]
		mask = keys[keys == key]
		values = self._values[column]
		return values.ix[mask.index].values[0]

	def reverse_lookup(self, column, value):
		'''Lookup key for given value with given column

		Args:
			column (column name): Name of column to look in.
			value (int or float): Number to search for within column.

		Returns:
			Key equivalent of value. 
		'''

		vals = self._values[column]
		mask = vals[vals == value]
		keys = self._keys[column]
		return keys.ix[mask.index].values[0]

	def regex_lookup(self, column, regex, ignore_case=True):
		'''Regular expression lookup for key within given column

		Args:
			lut (SparseLut)
			column (column name): Name of column to look in.
			regex (key): Pattern to search for within column.
			ignore_case (bool, optional): Ignore case. Default: True.

		Returns:
			Numeric equivalents of regular expression matches. 
		'''

		reg = re.compile(regex)
		if ignore_case:
			reg = re.compile(regex, re.IGNORECASE)
		keys = self._keys[column]
		mask = keys.apply(lambda x: reg.search(str(x)))
		mask = mask.astype(bool)
		mask = mask[mask == True]
		values = self._values[column]
		return values.ix[mask.index].values
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)
# ------------------------------------------------------------------------------

__all__ = []

if __name__ == '__main__':
	main()