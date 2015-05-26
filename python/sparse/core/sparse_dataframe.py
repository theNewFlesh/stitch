#! /usr/bin/env python
# Alex Braun 01.18.2015

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
	01.18.2015

Platform:
	Unix

Author:
	Alex Braun <ABraunCCS@gmail.com> <http://www.AlexBraunVFX.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
import re
from itertools import *
from collections import OrderedDict
import pandas
from pandas import DataFrame, Series
import numpy
from sparse.utilities.utils import *
from sparse.core.spql_interpreter import SpQLInterpreter
from sparse.core.sparse_series import SparseSeries
# from sparse.core.sparse_string import SparseString
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
		data = self.data.apply(lambda x: SparseSeries(x).nan_to_bottom())

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

	def flatten(self, columns=None, prefix=True, drop=True, dtype=dict, attach='inplace', inplace=False):
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
		def _reorder_columns(columns, index):
			new_cols = []
			for col in columns:
				if col in index:
					if not drop:
						new_cols.append(col)
					new_cols.extend( index[col] )
				else:
					new_cols.append(col)
			return new_cols

		col_index = OrderedDict()
		def _flatten(data, columns):
			for col in columns:
				col_index[col] = [] 
			frames = []
			for col in columns:
				frame = DataFrame(data[col].tolist())
				if prefix:
					columns = {}
					for k in frame.columns:
						columns[k] = str(col) + '_' + str(k)
					frame.rename(columns=columns, inplace=True)
				frames.append(frame)
				col_index[col].extend( frame.columns.tolist() )
			data = pandas.concat(frames, axis=1)
			return data
		
		data = self.data
		flatdata = data
		old_cols = data.columns.tolist()

		# determine flatenable columns via column mask
		if columns:
			flatdata = flatdata[columns]
		else:
			mask = data.applymap(lambda x: bool_test(type(x), '==', dtype))
			iterables = data[mask]
			iterables = iterables.dropna(how='all', axis=1)
			columns = iterables.columns.tolist()
		
		# Get right-hand flattened columns
		flatdata = _flatten(flatdata, columns)
		
		old_cols = data.columns.tolist()

		# drop original columns
		if drop:
			data = data.T.drop(columns).T

		# attach right-hand flattened columns to  original columns
		data = pandas.concat([data, flatdata], axis=1)

		# reorganize columns
		if attach == 'inplace':
			cols = _reorder_columns(old_cols, col_index)
			data = data[cols]

		if inplace:
			self.data = data
		return data

	def drop_columns(self, columns, inplace=False):
		'''Drop a given set of columns from the DataFrame

		Args:
			columns (list): Set of columns to drop.
			inplace (bool, optional): Apply changes in place. Default: False
		
		Returns: 
			Reduced DataFrame

		Example:
			>>> sdf.data
			   a  b   c   d
			0  0  1   2   3
			1  4  5   6   7
			2  8  9  10  11

			>>> sdf.drop_columns(['a', 'c'])
			   b   d
			0  1   3
			1  5   7
			2  9  11
		'''
		data = self.data
		data = data.T.drop(columns).T

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
			column (column name): Column by which to split data into chunks
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

		Example:
			>>> sdf.data
			    make  model color  year
			0    gmc    suv  blue  2007
			1  honda    suv  blue  2007
			2   fiat    car  blue  2007
			3    gmc  truck  blue  1999

			>>> sdf.unique()
			    make  model color  year
			0    gmc    suv  blue  2007
			1  honda    car   NaN  1999
			2   fiat  truck   NaN   NaN
		'''
		data = self.data	
		mask = data.apply(lambda x: x.duplicated())
		data[mask] = numpy.nan
		data = data.apply(lambda x: SparseSeries(x).nan_to_bottom())
		data = data.dropna(how='all')

		if inplace:
			self.data = data
		return data

	def cross_map(self, source_column, target_column, source_predicate, 
				  target_predicate=None, inplace=False):
		'''Applies a predicate to a target column based on a mask derived from 
		the results of a source predicate applied to a source column

		Args:
			source_column (column name): Column by which to derive a mask
			target_column (column name): Column to be changed
			source_predicate (lambda or func): Rule by which to create a mask, (ie lambda x: x == 'foo').
			target_predicate (lambda or func): Rule by which to change target column, (ie lambda x: 'bar')
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			Cross-mapped DataFrame

		Example:
			>>> sdf.data
			    make  model color  year
			0    gmc    suv  blue  2007
			1  honda    suv  blue  2007
			2   fiat    car  blue  2007
			3    gmc  truck  blue  1999

			>>>	sdf.cross_map('year', 'color', lambda x: x == 2007, lambda x: 'red')
			    make  model color  year
			0    gmc    suv   red  2007
			1  honda    suv   red  2007
			2   fiat    car   red  2007
			3    gmc  truck  blue  1999
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

	def group_cross_map(self, group_column, value_column, source_column, 
						target_column, predicate, concat=True, inplace=False):
		'''Applies a predicate to a target column based on a mask derived from 
		the results of a source predicate applied to a source column

		Args:
			group_column (column name): Column by which to group data
			value_column (str): New column for storing concatenated results
			source_column (column name): Column by which to derive a mask
			target_column (column name): Column to be changed
			predicate (lambda or func): Rule by which to create a mask, (ie lambda x: x == 'foo')
			concat (bool, optional): Concatenate results into comma separated string. Default: True
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame with new value column

		Example:
			>>> sdf.data
			  last_name first_name
			0   schmidt        tom
			1   schmidt       dick
			2   schmidt      harry
			3   flately      shane
			4   flately       anne
			5     klein      ernst
			6     klein        max
			7     klein     janice
			
			>>>  sdf.group_cross_map('last_name', 'relatives', 'last_name',
                    				 'first_name', lambda x: True)
			  last_name first_name           relatives
			0   schmidt        tom    tom, dick, harry
			1   schmidt       dick    tom, dick, harry
			2   schmidt      harry    tom, dick, harry
			3   flately      shane         shane, anne
			4   flately       anne         shane, anne
			5     klein      ernst  ernst, max, janice
			6     klein        max  ernst, max, janice
			7     klein     janice  ernst, max, janice

			>>> sdf.group_cross_map('last_name', 'relatives', 'last_name',
                    				'first_name', lambda x: x != 'flately')
			  last_name first_name           relatives
			0   schmidt        tom    tom, dick, harry
			1   schmidt       dick    tom, dick, harry
			2   schmidt      harry    tom, dick, harry
			3   flately      shane                    
			4   flately       anne                    
			5     klein      ernst  ernst, max, janice
			6     klein        max  ernst, max, janice
			7     klein     janice  ernst, max, janice
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

	def merge_columns(self, columns, func='default', new_column='default', 
					iterables=False, drop=False, inplace=False):
		'''Merge columns according to supplied or default function

		Args:
			columns (list): Columns to be merged
			func (func optional): Func used to merge columns. Default: default
			new_column (str optional): Name of merged column. Default: merge_a_b...
			iterables (bool optional): True if any column contains an iterable. Default: False
			drop (bool optional): Drop columns to be merged. Default: False
			inplace (bool, optional): Apply changes in place. Default: False

		Example:
			>>> print sdf.data		
			  first_name last_name  age    job_1    job_2
			0       john   jenkins   23    pilot     None
			1       jane     smith   46  surgeon     None
			2      harry    harmon   27  teacher  chemist
			3        sue     marie   78    nurse    baker

			>>> print sdf.merge_columns(['first_name', 'last_name'])
			  first_name last_name  age    job_1    job_2 merged_first_name_last_name
			0       john   jenkins   23    pilot     None                johnjenkins
			1       jane     smith   46  surgeon     None                  janesmith
			2      harry    harmon   27  teacher  chemist                harryharmon
			3        sue     marie   78    nurse    baker                   suemarie

			>>> print sdf.merge_columns(['job_1', 'job_2'], 
				func=lambda x: [ x[x.index[0]], x[x.index[1]] ],
				new_column='jobs')

			  first_name last_name  age    job_1    job_2                jobs
			0       john   jenkins   23    pilot     None       [pilot, None]
			1       jane     smith   46  surgeon     None     [surgeon, None]
			2      harry    harmon   27  teacher  chemist  [teacher, chemist]
			3        sue     marie   78    nurse    baker      [nurse, baker]

			>>> print sdf.merge_columns(['job_1', 'job_2'], 
				func=lambda x: {'1st': x[x.index[0]], '2nd': x[x.index[1]] },
				new_column='jobs', drop=True)
			  first_name last_name  age                                      jobs
			0       john   jenkins   23          {u'2nd': None, u'1st': u'pilot'}
			1       jane     smith   46        {u'2nd': None, u'1st': u'surgeon'}
			2      harry    harmon   27  {u'2nd': u'chemist', u'1st': u'teacher'}
			3        sue     marie   78      {u'2nd': u'baker', u'1st': u'nurse'}

		Returns: 
			DataFrame
		'''
		def _add(item):
			output = item[item.index[0]]
			for i, col in enumerate(columns[1:]):
				output += item[item.index[i + 1]]
			return output
		
		def _append(item):
			output = []
			for i, col in enumerate(columns):
				output.append(item[item.index[i]])
			return output
		
		if func == 'default':
			func = _add
			if iterables:
				func = _append
		
		data = self.data.copy()
		result = data[columns]
		result = result.T.apply(lambda x: [func(x)]).T
		result = result.apply(lambda x: x[0])
		
		col = 'merged_' + '_'.join([str(x) for x in columns])
		if new_column != 'default':
			col = new_column
		data[col] = result

		if drop:
			for col in columns:
				del data[col]

		if inplace:
			self.data = data
		return data

	def to_inverted_index(self, columns, key, prototype=True):
		'''Converts a list of columns containing dict or dict matrices to an inverted index

		Example:
			>>> sdf.data

		'''
		index = []
		for col in columns:
			data = self.data[col]
			index = data[data.apply(lambda x: is_dictlike(x))].tolist()
			lists = data[data.apply(lambda x: is_listlike(x))].tolist()
			for lst in lists:
				for item in lst:
					index.append(item)
		return to_inverted_index(index, key, prototype)

	def merge_list_dict_columns(self, source, target, source_key, target_key,
							new_column='default', remove_key=False, drop=False, 
							inplace=False):
		'''Merge columns containing lists of dicts

		Args:
			source (source column): Source column
			target (target column): Target column
			source_key (key): Source dict key to merge on
			target_key (key): Target dict key to merge on
			new_column (str, optional): Name of new merge column
			remove_key (bool, optional): Remove merge keys from

			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame
		'''
		func = lambda x: merge_list_dicts( x[x.index[0]], x[x.index[1]],
										   source_key, target_key, remove_key=remove_key)

		data = self.merge_columns([source, target], func=func, new_column=new_column, drop=drop)
		
		if inplace:
			self.data = data
		return data

	def traverse(self, key_func, val_func, traverse='depth', replace=False, inplace=False):
		data = self.data.copy()
		
		spindex = data.columns.tolist()
		spindex = [x for x in spindex if re.search('sp_index', x)]
		
		index = data[spindex]
		_index = index.as_matrix().tolist()
		if traverse == 'breadth':
			index = data[spindex].transpose()
		index = index.as_matrix().tolist()

		for r, row in enumerate(index):
			for k, key in enumerate(row):
				if key_func(key):

					ind = r
					col = k
					if traverse == 'breadth':
						ind = k
						col = r
					
					try:
						data.loc[ind, 'values'] = val_func(data.loc[ind, 'values'])
					except:
						continue
							
					if replace:
						column = spindex[col]
						mask = data[column].apply(lambda x: x != key)
						mask.ix[ind] = True
						data = data[mask]
						
						branch = _index[ind][0:col + 1]
						while len(branch) < len(spindex):
							branch.append('-->')
						data.loc[ind, spindex] = branch 
						break
						
		if inplace:
			self.data = data
		return data   
	# --------------------------------------------------------------------------
	
	def read_nested_dict(self, item, sp_index=False, justify='left', inplace=False):
		'''Reads nested dictionary into a DataFrame

		Args:
			item (dict): Dictionary to be read
			name (str): Name of dictionary
			inplace (bool, optional): Apply changes in place. Default: False

		Returns: 
			DataFrame
		'''
		values = flatten_nested_dict(item).values()
		data = None
		if sp_index:
			index = nested_dict_to_matrix(item, justify=justify)
			columns = []
			for i, item in enumerate(index[0]):
				columns.append("sp_index_" + str(i).zfill(2))
			data = DataFrame(index, columns=columns)
		else:
			index = nested_dict_to_index(item, justify=justify)
			data = DataFrame(index=index)
		data['values'] = values
		mask = data.apply(lambda x: x != 'null')
		mask = mask[mask].dropna()
		data = data.ix[mask.index]
		if sp_index:
			data.reset_index(drop=True, inplace=True)

		if inplace:
			self.data = data
		return data

	def to_nested_dict(self):
		if 'sp_index_00' in self.data.columns:
			temp = self.data.values.tolist()
			matrix = []
			for row in temp:
				matrix.append([x for x in ifilter(lambda x: x != '-->', row)])
			return matrix_to_nested_dict(matrix)

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

		Search:			
					   query: (<field>, <field> ...) <operator> (<value>, <value> ...)
			multiple queries: <query> | <query> | <query> ...
					 example: (name) contains (jupiter)
					 example: (name) notcont (scratch, test)
					 example: (priority) < (2501)
					 example: (name) contains (jupiter) | (name) notcont (scratch, test) | (priority) < (2501)

		Field:			
			A field is a known key of a database item.
			
			For instance, if a database exists as a table, then the column headers
			would count as fields.  A database of people might have the keys:
			name, height, weight and age.
			
			All fields (even single ones), must be surrounded by parenthesis
			use double quotes ("") to capture spaces and other special characters

		Value:			
			A value is the, often unknown, datum associated with the key of a
			database item.
			
			For instance, if a database exists as a table, then the data
			contained in the body would count as values.  A database of people
			might have an item with the keys: name, height, weight, age, with
			values of say Steve, 180", 210lbs, 25.
			
			All values (even single ones), must be surrounded by parenthesis
			use double quotes ("") to capture spaces and other special characters

		Operator:			
			An operator is a symbol which denotes an operation to be performed
			on a given database with the left and right operands (field and values)
			as its arguments.
			
			For instance, in arithematic, the addition operator (+) combines
			its two operands into a single value (2 + 2 = 4).  In SpQL, an
			operator peforms a test on the values of the fields denoted as
			the left operands, using the values denoted in the right operands
			as criteria.  So, "(name) contains (jupiter)" searches all the
			items in a database whose names contain the string "jupiter".
			
			Operators include:
										  is,   =  :  exact match
						   is not,     isnot,  !=  :  does not match
					 greater than,        gt,   >  :  greater than
			greater than equal to,       gte,  >=  :  greater than or equal to
						less than,        lt,   <  :  less than
			   less than equal to,       lte,  <=  :  less than or equal to
						 contains,      cont,   ~  :  contains
				 does not contain,   notcont,  !~  :  does not contain
					   cscontains,    cscont,  ~~  :  contains (case sensitive)
			   does not cscontain, csnotcont, !~~  :  does not contain (case sensitive)

		And:			
			The AND operator (&) is a means of chaining queries together.  It pipes the
			results of the left query into the right query as its new, smaller
			database.

		Or:			
			The OR operator (|) is a means of concatenating multiple queries into a
			single result.  Both operands are executed as independent queries and
			their results are then merged together with duplicate rows removed.
		'''
		self._spql.search(string)
		data = self._spql.dataframe_query(self.data, field_operator=field_operator)

		if inplace:
			self.data = data
		return data

	# def parse(self, columns, parse_index, verbose=False, inplace=True):
	# 	'''Parse column elements according to parse index

	# 	Args:
	# 		columns (list): Columns to be parsed
	# 		parse_index (dict): A nested dictionary formatted for a SparseString
	# 		inplace (bool, optional): Apply changes in place. Default: False

	# 	Returns: 
	# 		Parsed DataFrame
	# 	'''

	# 	string = SparseString(parse_index, verbose=verbose)
	# 	data = self.data
	# 	data[columns] = data[columns].applymap(string.parse)

	# 	if inplace:
	# 		self.data = data
	# 	return data
	# --------------------------------------------------------------------------
	
	def plot(self, embedded_column=None, ax=None, colormap=None, figsize=None, 
			 fontsize=None, grid=None, kind='line', legend=True, loglog=False, 
			 logx=False, logy=False, mark_right=True, rot=None, secondary_y=False,
			 sharex=True, sharey=False, sort_columns=False, stacked=False, style=None,
			 subplots=False, table=False, title=None, use_index=True, x=None, xerr=None,
			 xlabel=None, xlim=None, xticks=None, xtick_labels=None, y=None, yerr=None,
			 ylabel=None, ylim=None, yticks=None, ytick_labels=None, 
			 bbox_to_anchor=(0.99, 0.99), loc=0, borderaxespad=0., **kwds):

		plot(self.data, embedded_column=embedded_column, ax=ax, colormap=colormap, 
			 figsize=figsize, fontsize=fontsize, grid=grid, kind=kind, legend=legend,
			 loglog=loglog, logx=logx, logy=logy, mark_right=mark_right, rot=rot,
			 secondary_y=secondary_y, sharex=sharex, sharey=sharey,
			 sort_columns=sort_columns, stacked=stacked, style=style,
			 subplots=subplots, table=table, title=title, use_index=use_index,
			 x=x, xerr=xerr, xlabel=xlabel, xlim=xlim, xticks=xticks,
			 xtick_labels=xtick_labels, y=y, yerr=yerr, ylabel=ylabel, ylim=ylim,
			 yticks=yticks, ytick_labels=ytick_labels, bbox_to_anchor=bbox_to_anchor,
			 loc=loc, borderaxespad=borderaxespad, **kwds)
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
