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
import warnings
from collections import OrderedDict
from itertools import *
import json
import pandas
from pandas import DataFrame, Series, Panel
import numpy
from sparse.utilities.utils import *
from sparse.utilities.errors import *
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

	def __init__(self, data=None, null='missing data', null_value=-1.0, name=None):
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
		self._keys = None
		self._values = None
		if type(data) != None:
			self.ingest(data)

	def _reduce_keys(self):
		'''Semi-private method for reducing the keys table'''

		keys = self._keys
		keys.unique(inplace=True)
		keys.data = keys.data.applymap(lambda x: self._null if pandas.isnull(x) else x)

	def ingest(self, data):
		self._keys = SparseDataFrame(data, name='keys')
		self._reduce_keys()
		self.generate_values()

	def generate_values(self, start=1, step=1):
		'''Generate a table of floating point values according to the keys table 

		Args:
			start (int, optional): Minimum floating point value. Default: 1.
			step (int, optional): Amount to step between values. Default 1.
		'''
		data = self._keys.data.copy()
		mask = data.applymap(lambda x: bool_test(x, '==', [self._null]))
		x, y = data.shape
		x += start
		vals = cycle(range(start, x, step))
		data = data.applymap(lambda x: vals.next())
		data[mask] = self._null_value
		data = data.applymap(lambda x: float(x))
		self._values = SparseDataFrame(data, name='values')

	def read_json(self, string, keys_only=True, orient='records'):
		if keys_only:
			data = SparseDataFrame(name='keys')
			data.read_json(string, orient=orient)
			self._keys = data
			self._reduce_keys()
			self.generate_values()
		else:
			data = json.loads(string, orient=orient)
			self._keys = SparseDataFrame(data['keys'], name='keys')
			self._values = SparseDataFrame(data['values'], name='values')

	def to_json(self, keys_only=True, orient='records'):
		if keys_only:
			return self._keys.to_json(orient=orient)
		else:
			output = {}
			output['keys'] = self._keys.data.to_dict()
			output['values'] = self._values.data.to_dict()
			return json.dumps(output, orient=orient)
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

	def spql_lookup(self, string, reverse=False, return_dataframe=False, verbosity=None):
		'''Lookup numeric value for given key with given column

		Args:
			string (str): SpQL search string. Example: (name) = (jack)
			reverse (bool, optional): Reverse lookup. Default: False
			return_dataframe (bool, optional): return a DataFrame instead of a raw value
			verbosity (str, optional): Level of verbosity (error, warn or None). Default: None

		Returns:
			Numeric equivalent of key. 
		'''
		
		output = None
		if reverse:
			output = self._values.spql_search(string)
			columns = self._values._spql.last_search[0][0]['fields']
			index = output.dropna(how='all').index
			output = self._keys.data.loc[index, columns]
		else:
			output = self._keys.spql_search(string)
			columns = self._keys._spql.last_search[0][0]['fields']
			index = output.dropna(how='all').index
			output = self._values.data.loc[index, columns]
			
		if len(output) == 0:
			message = 'No search results found. SpQL search: ' + string
			if verbosity == 'error':
				raise NotFound(message)
			if verbosity == 'warn':
				warnings.warn(message, Warning)
			return numpy.nan

		if not return_dataframe:
			output = output.values.tolist()[0][0]
		return output

	def transform_items(self, items, source_column, target_column, operator='=', verbosity=None):
		'''Lookup numeric value for given key with given column

		Args:
			items: items to be transformed
			source_column: source column
			target_column: target column
			operator (str, optional): operator to search with (=, ~, ~~). Default '='
			verbosity (str, optional): Level of verbosity (error, warn or None). Default: None

		Returns:
			Numeric equivalent of key. 
		'''

		def _transform_item(item):
			source = '(' + source_column + ') ' + operator + ' (' + item + ')'
			found = self.spql_lookup(source, verbosity=verbosity)
			if pandas.notnull(found):
				target = '(' + target_column + ') ' + operator + ' (' + str(found) + ')'
				new_item = self.spql_lookup(target, reverse=True, verbosity=verbosity)
				return new_item
			else:
				return item

		if is_iterable(items):
			return [_transform_item(item) for item in items]
		else:
			return _transform_item(item)

	def make_numerical(self, data, spql=False):
		data = data.copy()
		columns = data.columns.tolist()
		for col in columns:
			data[col] = data[col].apply(lambda x: self.lookup_item(x, col, spql=spql))
		return data

	def lookup_item(self, item, column, spql=False, operator='=', verbosity=None):
		output = numpy.nan
		if spql:
			search = '(' + str(column) + ') ' + operator + ' (' + str(item) + ')'
			output = self.spql_lookup(search, verbosity=verbosity)
			return output
		
		mask = self._keys.data[column].apply(lambda x: x == item)
		result = self._values.data[mask]
		result = result[column]
		if not result.empty:
			output = result.tolist()[0]
			return output
		else:
			message = 'No results found. item: ' + str(item) + ' column: ' + str(column)
			if verbosity == 'error':
				raise NotFound(message)
			if verbosity == 'warn':
				warnings.warn(message, Warning)
			return numpy.nan
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