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
from pandas import DataFrame, Series, Panel
import pandas
import numpy
from matplotlib import pyplot
import os
import json
import re
import copy
from collections import *
from itertools import *
from Axon.Utilities.ErrorsImpl import *
# ------------------------------------------------------------------------------

class LUT(Panel):
	def _reduce_data(self, data):
		data = data.reset_index()
		del data['index']
		data = data.apply(unique)
		data = data.apply(organize)
		data[data.apply(pandas.isnull)] = self._null
		data = data.apply(unique)
		data = data.dropna(how='all')
		return data

	def _generate_values(self, data):
		values = data.apply(lambda x: range(data.shape[0]))
		values[data.apply(pandas.isnull)] = numpy.nan
		values = data_map.astype(float)
		values[data == self._null] = self._null_value
		return values
	# --------------------------------------------------------------------------

	def __init__(self, data, null='Missing Data', null_value=-1.0):
		super(LUT, self).__init__()
		self._null = null
		self._null_value = null_value
		data = self._reduce_data(data)
		values = self._generate_values(data)

	def lookup(self, lut, column, datum):
		data = lut['data'][column]
		mask = data[data == datum]
		values = lut['values'][column]
		return values.ix[mask.index].values[0]

	def reverse_lookup(self, lut, column, value):
		values = lut['values'][column]
		mask = values[values == value]
		data = lut['data'][column]
		return data.ix[mask.index].values[0]

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

class SD_dataframe(DataFrame):

	def reduce(self, data):
		data = data.reset_index()
		del data['index']
		data = data.apply(unique)
		data = data.apply(organize)
		data[data.apply(pandas.isnull)] = self._null
		data = data.apply(unique)
		data = data.dropna(how='all')
		return data

class SD_series(Series):

	def organize(self, series):
		new_series = series.dropna()
		buffer = [numpy.nan] * (series.size - new_series.size)
		new_series = new_series.append(Series(buffer))
		return Series(list(new_series), index=series.index)


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