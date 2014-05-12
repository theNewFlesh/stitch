 #! /usr/bin/env python

# Alex Braun 11.13.2013
# >> INSERT LICENSE HERE <<

'''
.. module:: SparseData implementation
	:date: 11.13.2013
	:platform: Unix
	:synopsis: Library of commands for converting sparse data into tabular data

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
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
	def __init__(self, data, null='missing data', null_value=-1.0, name=None):
		super(SparseLUT, self).__init__(name=name)
		self._cls = 'SparseLUT'

		self._null = null
		self._null_value = null_value
		self._keys = data
		self._reduce_keys()
		self._values = None
		self.generate_values()

	def _reduce_keys(self):
		data = SparseDataFrame(self._keys)
		data.unique(inplace=True)
		data.nan_to_bottom(inplace=True)
		data = data.data
		data.dropna(how='all', inplace=True)
		data[data.apply(pandas.isnull)] = self._null
		self._keys = data
	
	def generate_values(self, start=0, step=1):
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
		return self._keys

	@property
	def values(self):
		return self._values
	# --------------------------------------------------------------------------

	def lookup(self, column, datum):
		keys = self._keys[column]
		mask = keys[keys == datum]
		values = self._values[column]
		return values.ix[mask.index].values[0]

	def reverse_lookup(self, column, value):
		vals = self._values[column]
		mask = vals[vals == value]
		keys = self._keys[column]
		return keys.ix[mask.index].values[0]

	def regex_lookup(self, lut, column, regex='', ignore_case=True):
		reg = re.compile(regex)
		if ignore_case:
			reg = re.compile(regex, re.IGNORECASE)
		data = lut['data'][column]
		mask = data.apply(lambda x: reg.search(str(x)))
		mask = mask.astype(bool)
		mask = mask[mask == True]
		values = lut['values'][column]
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