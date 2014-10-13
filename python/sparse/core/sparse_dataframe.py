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

'''The sparse_dataframe module contains the SparseDataFrame class

The SparseDataFrame class is an extension of the DataFrame class from the Pandas
library.  SparseDataFrames contain additional methods for converting sparse data,
such as esoteric databases, logs and custom made tables, into SparseDataFrames,
with the actual data residing within a Pandas DataFrame.  

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
from sparse.core.spql_interpreter import SpQLInterpreter
from sparse.core.sparse_string import SparseString
# ------------------------------------------------------------------------------

class SparseDataFrame(Base):
	'''Class for converting sparse data into well-formated, tabular data

	The SparseDataFrame class which is an extension of the DataFrame class from
	the Pandas library.  SparseDataFrames contain additional methods for converting
	sparse data, such as esoteric databases, logs and custom made tables, into
	SparseDataFrames, with the actual data residing	within a Pandas DataFrame,
	acessible through the data attribute.

	Attributes:
		cls (str): Class descriptor
		name (str): Name descriptor
		data (DataFrame): Internal DataFrame where data is actually stored

	Example:
		>>> data = [[ 'joe',  12, 'mechanic'],
        >>> ['bill',  22,  'soldier'],
        >>> [ 'sue',  65,    'pilot'],
        >>> ['jane',  43,  'teacher']]
		
		>>> columns = ['name', 'age', 'profession']
		
		>>> sdf = SparseDataFrame(data=data, columns=columns)
		
		>>> print sdf
		<sparse.core.sparse_dataframe.SparseDataFrame object at 0x10accfed0>
		
		>>> print sdf.data
		   name  age profession
		0   joe   12   mechanic
		1  bill   22    soldier
		2   sue   65      pilot
		3  jane   43    teacher
	'''

	def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False, name=None):
		'''SparseDataFrame initializer

		Args:
			data (array-like or DataFrame, optional): Data to be ingested. Default: None
			index (Index or array-like, optional): Index of dataframe. Default: None
			columns (Index or array-like, optional): Column names. Default: None
			dtype (dtype, optional): Data type to force, otherwise infer. Default: None
			copy (bool, optional): Copy data from inputs. Default: None
			name (str, optional): Name of object. Default: None

		Returns:
			SparseDataFrame
		'''

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
		'''Converts data to specified type, leaving uncoercible types alone

		Args:
			dtype (type): Type to convert data into.
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher

			>>>	print sdf.data.applymap(type)
				       name                   age    profession
			0  <type 'str'>  <type 'numpy.int64'>  <type 'str'>
			1  <type 'str'>  <type 'numpy.int64'>  <type 'str'>
			2  <type 'str'>  <type 'numpy.int64'>  <type 'str'>
			3  <type 'str'>  <type 'numpy.int64'>  <type 'str'>

		    >>> sdf.to_type(str, inplace=True)
			>>> print sdf.data.applymap(type)
			           name           age    profession
			0  <type 'str'>  <type 'str'>  <type 'str'>
			1  <type 'str'>  <type 'str'>  <type 'str'>
			2  <type 'str'>  <type 'str'>  <type 'str'>
			3  <type 'str'>  <type 'str'>  <type 'str'>
		'''

		data = self.data.applymap(lambda x: to_type(x, dtype))

		if inplace:
			self.data = data
		return data

	def is_iterable(self, inplace=False):
		'''Returns a boolean mask indicating which elements are iterable

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame mask

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher

			>>> mask = sdf.is_iterable()
			>>> print mask
			   name    age profession
			0  True  False       True
			1  True  False       True
			2  True  False       True
			3  True  False       True

			>>> print sdf.data[mask]
			   name  age profession
			0   joe  NaN   mechanic
			1  bill  NaN    soldier
			2   sue  NaN      pilot
			3  jane  NaN    teacher
		'''

		data = self.data.applymap(lambda x: is_iterable(x))

		if inplace:
			self.data = data
		return data

	def make_iterable(self, inplace=False):
		'''Makes all the elements iterable

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of iterable elements

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher

			>>> print sdf.make_iterable()
			   name   age profession
			0   joe  [12]   mechanic
			1  bill  [22]    soldier
			2   sue  [65]      pilot
			3  jane  [43]    teacher
		'''

		data = self.data.applymap(lambda x: make_iterable(x))

		if inplace:
			self.data = data
		return data

	def coerce_nulls(self, inplace=False):
		# TODO: make this method faster
		'''Coerce all null elements into numpy.nan

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of coerced elements

		Example:
			>>> print sdf.data
			   name age profession
			0   joe  12   mechanic
			1  bill  ()         {}
			2   sue  65       [{}]
			3        43    teacher

			>>> print sdf.coerce_nulls()
			   	   name  age profession
				0   joe   12   mechanic
				1  bill  NaN        NaN
				2   sue   65        NaN
				3   NaN   43    teacher
		'''

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
		'''Pushes all nan elements to the bottom rows of the DataFrame

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame with nan elements at the bottom

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   mechanic
			1  bill  NaN        NaN
			2   sue   65        NaN
			3   NaN   43    teacher

			>>> print sdf.nan_to_bottom()
			   name  age profession
			0   joe   12   mechanic
			1  bill   65    teacher
			2   sue   43        NaN
			3   NaN  NaN        NaN
		'''

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
		# May be deprecated in favor of spql_search
		'''Apply regular expression matches to all DataFrame elements

		Args:
			pattern (str): Regular expression pattern to match
			group (int, optional): Regular expression group to return. Default: 0
			ignore_case (bool, optional): Ignore case. Default: False
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of regex matches

		Example:
			>>> print sdf.data		
			   name  age         profession
			0   joe   12  Airplane Mechanic
			1  bill   22            soldier
			2   sue   65              pilot
			3  jane   43            teacher

			>>> print sdf.regex_match('airplane (mechanic)', group=1, ignore_case=True)
			   name  age profession
			0   joe   12   Mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher
		'''
		
		data = self.data.applymap(lambda x: regex_match(pattern, x, group=group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_search(self, pattern, group=0, ignore_case=False, inplace=False):
		# May be deprecated in favor of spql_search
		'''Apply regular expression searches to all DataFrame elements

		Args:
			pattern (str): Regular expression pattern to search
			group (int, optional): Regular expression group to return. Default: 0
			ignore_case (bool, optional): Ignore case. Default: False
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of regex searches

		Example:
			>>> print sdf.data		
			   name  age                      profession
			0   joe   12  Experimental Airplane Mechanic
			1  bill   22                         soldier
			2   sue   65                           pilot
			3  jane   43                         teacher

			>>> print sdf.regex_search('airplane (mechanic)', group=1, ignore_case=True)
			   name  age profession
			0   joe   12   Mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher
		'''

		data = self.data.applymap(lambda x: regex_search(pattern, x, group=group, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_sub(self, pattern, repl, count=0, ignore_case=False, inplace=False):
		'''Apply regular expression substitutions to all DataFrame elements

		Args:
			pattern (str): Regular expression pattern to substitute
			repl (str): String to replace pattern
			group (int, optional): Regular expression group to return. Default: 0
			count (int, optional): Maximum number of occurences to be replaced. Default: 0
			ignore_case (bool, optional): Ignore case. Default: False
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of regex substitutions

		Example:
			>>> print sdf.data
			   name  age         profession
			0   joe   12  Airplane Mechanic
			1  bill   22            soldier
			2   sue   65              pilot
			3  jane   43            teacher

			>>> print sdf.regex_sub('airplane', 'Helicopter', ignore_case=True)
			   name  age           profession
			0   joe   12  Helicopter Mechanic
			1  bill   22              soldier
			2   sue   65                pilot
			3  jane   43              teacher
		'''

		data = self.data.applymap(lambda x: regex_sub(pattern, repl, x, count=count, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data

	def regex_split(self, pattern, ignore_case=False, inplace=False):
		'''Splits elements into list of found regular expression groups

		Args:
			pattern (str): Regular expression pattern with groups, ie. "(foo)(bar)"
			ignore_case (bool, optional): Ignore case. Default: False
			inplace (bool, optional): Apply changes in place. Default: False

		Returns:
			DataFrame of with matched elements as lists of groups

		Example:
			>>> print sdf.data
			   name  age           profession
			0   joe   12  Helicopter Mechanic
			1  bill   22              soldier
			2   sue   65     helicopter pilot
			3  jane   43              teacher

			>>> sdf.regex_split('(helicopter) (.*)', ignore_case=True, inplace=True)
			>>> print sdf.data
			   name  age              profession
			0   joe   12  [Helicopter, Mechanic]
			1  bill   22                 soldier
			2   sue   65     [helicopter, pilot]
			3  jane   43                 teacher
		'''

		data = self.data.applymap(lambda x: regex_split(pattern, x, ignore_case=ignore_case))

		if inplace:
			self.data = data
		return data
	# --------------------------------------------------------------------------

	def flatten(self, dtype=dict, prefix=True, inplace=False):
		'''Split items of iterable elements into separate columns

		Args:
			dtype (type, optional): Columns types to be split. Default: dict
			prefix (bool, optional): Append original column name as a prefix to new columns
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Flattened DataFrame

		Example:
			>>> print sdf.data
			                   foo             bar
			0  {u'a': 1, u'b': 10}     some string
			1  {u'a': 2, u'b': 20}  another string
			2  {u'a': 3, u'b': 30}            blah

			>>> sdf.flatten(inplace=True)
			>>> print sdf.data
				foo_a 	 foo_b   	       bar
			0  		1 		10     some string
			1  		2 		20  another string
			2  		3 		30            blah
		'''

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
		'''Drops entire rows or columns from data according to given mask

		Args:
			how (drop method, optional): Drop method, ('any' or 'all')
										 Default: 'all'
			axis (int, optional): Axis to drop by. Default: 0
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Reduced DataFrame

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   mechanic
			1  bill   22    soldier
			2   sue   65      pilot
			3  jane   43    teacher

			>>> mask = sdf.is_iterable()
			>>> print sdf.drop_by_mask(mask, axis=1)
			   name  profession
			0   joe    mechanic
			1  bill     soldier
			2   sue       pilot
			3  jane     teacher
		'''

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
		'''Stacks data according to chunks demarcated by unique elements within 
		a given column

		This method is usefull for generating tables that can be easily graphed

		Args:
			column (column name): column by which to split data into chunks
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Stacked (striped) DataFrame

		Example:
			>>> print sdf.data
			   name  age profession
			0   joe   12   Mechanic
			1  bill   22    soldier
			2  bill   25  policeman
			3   sue   65      pilot
			4   sue   14    student
			5   sue   44      nurse
			6  jane   22   engineer
			7  jane   43    teacher

			>>> print sdf.stack_by_column('name')
			   joe        joe  bill       bill  sue        sue  jane       jane
			   age profession   age profession  age profession   age profession
			0   12   Mechanic    22    soldier   65      pilot    22   engineer
			1  NaN        NaN    25  policeman   14    student    43    teacher
			2  NaN        NaN   NaN        NaN   44      nurse   NaN        NaN
		'''

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
		'''Reduced striped DataFrame into DataFrame with unique columns

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Unstriped DataFrame

		Example:
			>>> print sdf.data
			  name profession  name profession  name profession  name profession
			0  joe   Mechanic  bill    soldier  bill  policeman  jane    teacher
			1  sue      pilot   NaN        NaN  jane   engineer   NaN        NaN

			>>> print sdf.unstripe()
			   name profession
			0   joe   Mechanic
			1   sue      pilot
			2  bill    soldier
			3   NaN        NaN
			4  bill  policeman
			5  jane   engineer
			6  jane    teacher
			7   NaN        NaN
		'''
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
		'''Returns a DataFrame of unique values, excluding numpy.nans

		Args:
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Unique DataFrame
		'''

		data = self.data
		mask = data.apply(lambda x: x.duplicated())
		data[mask] = numpy.nan

		if inplace:
			self.data = data
		return data

	def cross_map(self, source_column, target_column, source_predicate, target_predicate=None, inplace=False):
		'''Applies a predicate to a target column based on a mask derived from 
		the results of a source predicate applied to a source column

		Args:
			source_column (column name): column by which to derive a mask
			target_column (column name): column to be changed
			source_predicate (lambda or func): rule by which to create a mask, (ie lambda x: x == 'foo').
			target_predicate (lambda or func): rule by which to change target column, (ie lambda x: 'bar')
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Cross-mapped DataFrame
		'''

		data = self.data
		mask = data[source_column].apply(source_predicate)
		if target_predicate:
			data[target_column][mask] = data[target_column][mask].apply(target_predicate)
		else:
			data[target_column][mask] = data[source_column][mask]

		if inplace:
			self.data = data
		return data

	def group_cross_map(self, group_column, value_column, source_column, target_column, predicate,
						concat=True, inplace=False):
		'''Applies a predicate to a target column based on a mask derived from 
		the results of a source predicate applied to a source column

		Args:
			group_column (column name): column by which to group data
			value_column (str): new column for storing concatenated results
			source_column (column name): column by which to derive a mask
			target_column (column name): column to be changed
			predicate (lambda or func): rule by which to create a mask, (ie lambda x: x == 'foo')
			concat (bool, optional): Concatenate results into comma separated string. Default: True
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame with new value column
		'''

		data  = self.data
		data[value_column] = None
		mask = data[group_column].duplicated()
		groups = data[group_column][~mask].values.tolist()
		for group in groups:
			group_data = data[data[group_column] == group]
			mask = group_data[source_column].apply(lambda x: predicate(x))
			values = group_data[target_column][mask].values.tolist()
			if concat:
				values = ', '.join(values)
			data.loc[mask.index, value_column] = values

		if inplace:
			self.data = data
		return data

	def merge_list_dict_columns(self, source, target, source_key, target_key,
                            new_column='default', remove_key=False, inplace=False):
		'''Merge columns containing lists of dicts

		Args:
			source (source column): source column
			target (target column): target column
			source_key (key): source dict key to merge on
			target_key (key): target dict key to merge on
			new_column (str, optional): name of new merge column
			remove_key (bool, optional): remove merge keys from results
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame
		'''

	    data = self.data
	    merge_name = 'merge_' + str(source) + '_' + str(target)
	    source = data[source].apply(lambda x: [x])
	    target = data[target].apply(lambda x: [x]) 
	    merge_col = source + target
	    merge_col = merge_col.apply(lambda x: merge_list_dicts(x[0], x[1], source_key, target_key, remove_key=remove_key))
	    if new_column != 'default':
	        merge_name = new_column
	    data[merge_name] = merge_col
	    
	    if inplace:
	        self.data = data
	    return data    
	# --------------------------------------------------------------------------
	
	def read_nested_dict(self, item, name, inplace=False):
		'''Reads nested dictionary into a DataFrame

		Args:
			item (dict): Dictionary to be read
			name (str): Name of dictionary
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame
		'''

		values = flatten_nested_dict(item).values()
		index = nested_dict_to_index(item, name)
		data = DataFrame(values, index=index)
		mask = data.apply(lambda x: x != 'null')
		mask = mask[mask].dropna()
		data = data.ix[mask.index]

		if inplace:
			self.data = data
		return data

	def read_json(self, string, orient='records'):
		'''Reads json into SparseDataFrame

		Args:
			json (json): Json string to be read
			orient (str, optional): Schema of json. Default: 'orient'

		Returns: 
			None
		'''

		self.data = pandas.read_json(string, orient=orient)

	def to_json(self, orient='records'):
		'''Reads SparseDataFrame into json string

		Args:
			orient (str, optional): Schema of json. Default: 'orient'

		Returns: 
			JSON string
		'''

		return self.data.to_json(orient=orient)
	# --------------------------------------------------------------------------
	
	def spql_search(self, string, field_operator='==', inplace=False):
		'''Query data using the Sparse Query Language (SpQL)

		Args:
			string (str): SpQL search string
			field_operator (str): Advanced feature, do not use.  Default: '=='
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Queried (likely reduced) DataFrame

		Example:
			>>> print sdf.data
			    name  age
			0    abe   15
			1  carla   22
			2   jack   57

			>>> sdf.spql_search('(name) is (abe) | (age) < (50)', inplace=True)
			>>> print sdf.data
				name  age
			0    abe   15
			1  carla   22
		'''

		self._spql.search(string)
		data = self._spql.dataframe_query(self.data, field_operator=field_operator)

		if inplace:
			self.data = data
		return data

	def parse(self, columns, parse_index, verbose=False, inplace=True):
		'''Parse column elements according to parse index

		Args:
			columns (list): Columns to be parsed
			parse_index (dict): A nested dictionary formatted for a SparseString
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Parsed DataFrame
		'''

		string = SparseString(parse_index, verbose=verbose)
		data = self.data
		data[columns] = data[columns].applymap(string.parse)

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
