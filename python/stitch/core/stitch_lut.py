from __future__ import with_statement
import re
import warnings
from collections import OrderedDict
from itertools import *
import json
import pandas
from pandas import DataFrame, Series, Panel
import numpy
from stitch.core.utils import *
from stitch.core.errors import *
from stitch.core.stitch_frame import StitchFrame
# ------------------------------------------------------------------------------

'''The stitch_lut module contains the StitchLUT class.

The StitchLUT class is a lookup table used for converting non-numeric or
partially numeric data, into numeric data.

Platform:
	Unix

Author:
	Alex Braun <alexander.g.braun@gmail.com> <http://www.AlexBraunVFX.com>
'''

class StitchLUT(Base):
	'''Lookup table for non-numeric or partially numeric data

	Attributes:
		keys (DataFrame): Input data that has been reduced to unique values.
		values (DataFrame): Numeric values mapped to the keys.
	'''
	def __init__(self, data=None, null='missing data', null_value=-1.0):
		'''StitchLUT initializer

		Args:
			data (DataFrame): Keys data.
			null (string, optional): Placeholder for missing keys data. Default: 'missing data'.
			null_value (int, optional): Placeholder value for missing values data. Default: -1.0.
		'''
		self._null = null
		self._null_value = null_value
		self._keys = None
		self._values = None
		if type(data) != None:
			self.ingest(data)

	def _reduce_keys(self):
		'''Semi-private method for reducing the keys table'''
		keys = self._keys
		keys.unique()
		keys._data = keys._data.applymap(lambda x: self._null if pandas.isnull(x) else x)

	def ingest(self, data):
		'''
		Ingest data, create keys and generate values

		Args:
			data (DataFrame): DataFrame to be ingested.

		Returns:
			None
		'''
		self._keys = StitchFrame(data)
		self._reduce_keys()
		self.generate_values()

	def generate_values(self, start=1, step=1):
		'''Generate a table of floating point values according to the keys table

		Args:
			start (int, optional): Minimum floating point value. Default: 1.
			step (int, optional): Amount to step between values. Default 1.

		Returns:
			None
		'''
		data = self._keys._data.copy()
		mask = data.applymap(lambda x: bool_test(x, '==', [self._null]))
		x, y = data.shape
		x += start
		vals = cycle(range(start, x, step))
		data = data.applymap(lambda x: vals.next())
		data[mask] = self._null_value
		data = data.applymap(lambda x: float(x))
		self._values = StitchFrame(data)

	def read_json(self, string, keys_only=True, orient='records'):
		'''
		Read JSON data into keys and values of StitchFrame

		Args:
			string (str): JSON formatted string.
			keys_only (bool, optional): Read keys and generate values from them. Default: True.
			orient (str, optional): Type of JSON orientation. Default: 'records'.

		Returns:
			None
		'''
		if keys_only:
			data = StitchFrame()
			data.read_json(string, orient=orient)
			self._keys = data
			self._reduce_keys()
			self.generate_values()
		else:
			data = json.loads(string, orient=orient)
			self._keys = StitchFrame(data['keys'])
			self._values = StitchFrame(data['values'])

	def to_json(self, keys_only=True, orient='records'):
		'''Write internal data to a JSON string

		Args:
			keys_only (bool, optional): Write only keys. Default: True.
			orient (str, optional): JSON formatting type. Default 'records'.

		Returns:
			json
		'''
		if keys_only:
			return self._keys._data.to_json(orient=orient)
		else:
			output = {}
			output['keys'] = self._keys._data.to_dict()
			output['values'] = self._values._data.to_dict()
			return json.dumps(output, orient=orient)
	# --------------------------------------------------------------------------

	@property
	def keys(self):
		'''Keys table

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

	def stitch_lookup(self, string, reverse=False, return_dataframe=False, verbosity=None):
		'''Lookup numeric value for given key with given column

		Args:
			string (str): stitch search string. Example: (name) = (jack)
			reverse (bool, optional): Reverse lookup. Default: False
			return_dataframe (bool, optional): return a DataFrame instead of a raw value
			verbosity (str, optional): Level of verbosity (error, warn or None). Default: None

		Returns:
			Numeric equivalent of key.
		'''
		output = None
		if reverse:
			output = self._values.stitch_search(string)
			columns = self._values._spql.last_search[0][0]['fields']
			index = output.dropna(how='all').index
			output = self._keys._data.loc[index, columns]
		else:
			output = self._keys.stitch_search(string)
			columns = self._keys._spql.last_search[0][0]['fields']
			index = output.dropna(how='all').index
			output = self._values._data.loc[index, columns]

		if len(output) == 0:
			message = 'No search results found. stitch search: ' + string
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
			found = self.stitch_lookup(source, verbosity=verbosity)
			if pandas.notnull(found):
				target = '(' + target_column + ') ' + operator + ' (' + str(found) + ')'
				new_item = self.stitch_lookup(target, reverse=True, verbosity=verbosity)
				return new_item
			else:
				return item

		if is_iterable(items):
			return [_transform_item(item) for item in items]
		else:
			return _transform_item(item)

	def make_numerical(self, data, spql=False):
		'''Convert supplied data to its numerical equivalents determined by lookups.

		Args:
			data (DataFrame): DataFrame to be converted.
			spql (bool, optional): Use stitch to perform the lookup. Default: False.

		Returns
			DataFrame
		'''
		data = data.copy()
		columns = data.columns.tolist()
		for col in columns:
			data[col] = data[col].apply(lambda x: self.lookup_item(x, col, spql=spql))
		return data

	def lookup_item(self, item, column, spql=False, operator='=', verbosity=None):
		'''Lookup value of given item within a specified column.

		Args:
			item (item): Item to be use queried.
			column (str): Lookup column.
			spql (bool, optional): Use stitch to perform the lookup. Default: False.
			operator (str, optional): Comparison operator to be used in stitch query. Default: '='.
			verbosity (str, optional): Level of verbosity fro stitch query (error, warn or None). Default: None

		Returns:
			value
		'''
		output = numpy.nan
		if spql:
			search = '(' + str(column) + ') ' + operator + ' (' + str(item) + ')'
			output = self.stitch_lookup(search, verbosity=verbosity)
			return output

		mask = self._keys._data[column].apply(lambda x: x == item)
		result = self._values._data[mask]
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
