#! /usr/bin/env python
# Alex Braun 04.13.2014

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
.. module:: utils
	:date: 04.13.2014
	:platform: Unix
	:synopsis: General Python utilities

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import warnings
import re
from copy import copy
from decimal import Decimal
import numpy
import pandas
from pandas import DataFrame
from matplotlib import pyplot
from collections import OrderedDict, namedtuple
# ------------------------------------------------------------------------------

class Base(object):
	'''
	Generic base class
	'''
	def __init__(self, name=None):
		'''
		:kwarg name: name of instance
		:returns: Base instance
		:rtype: Base
		'''
		self._cls = 'Base'
		self._name = name

	@property
	def cls(self):
		'''
		class of instance
		'''
		return self._cls

	@property
	def name(self):
		'''
		name of instance
		'''
		return self._name

	def _print_public(self):
		'''Pretty print public methods and attributes
		'''
		nonPublicRE = re.compile('^_')
		for item in dir(self):
			found = nonPublicRE.match(item)
			if not found:
				print item

	def _print_semiprivate(self):
		'''Pretty print semi-private methods and attributes
		'''
		semiPrivateRE = re.compile('^_[^_]+')
		for item in dir(self):
			found = semiPrivateRE.match(item)
			if found:
				print item

	def _print_private(self):
		'''Pretty print private methods and attributes
		'''
		privateRE = re.compile('^__')
		for item in dir(self):
			found = privateRE.match(item)
			if found:
				print item
# ------------------------------------------------------------------------------

def to_type(item, dtype):
	'''Convert item to given type
	'''
	try:
		return dtype(item)
	except ValueError:
		return item

def is_iterable(item):
	'''Determine if item is iterable
	'''
	try:
		result = iter(item)
		return True
	except TypeError:
		return False

def make_iterable(item):
	'''Return item in a list if it is not already iterable
	'''
	if is_iterable(item):
		return item
	else:
		return [item]

def iprint(iterable):
	'''Print every item in iterable
	'''
	for item in iterable:
		print item

def set_decimal_expansion(item, expansion):
	'''Truncates a float item at specified number of digits after the decimal
	'''
	return int(item * 10 ** expansion) / float(10 ** expansion)

def try_(item, func):
	'''Wraps a supplied function in a try block
	'''
	try:
		return func(item)
	except:
		return item

def round_to(item, order):
	'''Rounds a given number to a given order of magnitudes after the decimal
	'''
	return round(Decimal(item), order)

def eval_(item):
	'''Evaluates item as expression
	'''
	try:
		return eval(item)
	except:
		return item
# ------------------------------------------------------------------------------

def _eq(item, value):
	return item == value

def _ne(item, value):
	return item != value

def _lt(item, value):
	return item < value

def _lte(item, value):
	return item <= value

def _gt(item, value):
	return item > value

def _gte(item, value):
	return item >= value

def _re(item, value):
	found = re.search(str(value), str(item))
	if found:
		return True
	else:
		return False

def _reig(item, value):
	found = re.search(str(value), str(item), flags=re.IGNORECASE)
	if found:
		return True
	else:
		return False

def _nre(item, value):
	found = re.search(str(value), str(item))
	if not found:
		return True
	else:
		return False

def _nreig(item, value):
	found = re.search(str(value), str(item), flags=re.IGNORECASE)
	if not found:
		return True
	else:
		return False

OPERATORS = {'==': _eq, '!=': _ne, '<': _lt, '<=': _lte, '>': _gt, '>=': _gte, 
			're': _re, 're.IGNORECASE': _reig, 'nre': _nre, 'nre.IGNORECASE': _nreig}

def bool_test(item, operator, values):
	'''Perform a boolean operation between an item and a given set of values
	'''
	op = OPERATORS[operator]
	values = make_iterable(values)
	for value in values:
		if op(item, value):
			return True
	return False
# ------------------------------------------------------------------------------

def regex_match(pattern, string, group=0, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, flags=re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = None
	try:
		found = regex.match(string)
	except TypeError:
		return string
	if found:
		return found.group(group)
	else:
		return string

def regex_search(pattern, string, group=0, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, flags=re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = None
	try:
		found = regex.search(string)
	except TypeError:
		return string
	if found:
		return found.group(group)
	else:
		return string

def regex_sub(pattern, repl, string, count=0, ignore_case=False):
	if ignore_case:
		try:
			return re.sub(pattern, repl, string, count=count, flags=re.IGNORECASE)
		except TypeError:
			return string
	else:
		try:
			return re.sub(pattern, repl, string, count=count)
		except TypeError:
			return string

def regex_split(pattern, string, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, flags=re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = None
	try:
		found = regex.search(string)
	except TypeError:
		return string
	if found:
		return list(found.groups())
	else:
		return string
# ------------------------------------------------------------------------------

def invert(iterable):
	'''Inverts a given iterable

	Example:
		>>> invert([1,2,3,1,2,3])
		[3,2,1,3,2,1]

		>>> invert(list('abc'))
		['c', 'b', 'a']

		>>> invert(list('abxy'))
		['y', 'x', 'b', 'a']
	'''
	patterns = sorted(list(set(iterable)))
	inversion_map = dict(zip(patterns, [x for x in reversed(patterns)]))
	return [inversion_map[x] for x in iterable]

def reduce_units(iterable, new_unit='-', min=0):
	'''Reduce unique items to multiples of a new unit

	Example:
		This function is usefull for simplifying indentation
		>>> for indent, line in zip(indents, text):
		>>>		print indent, line 
		First
		  Second
				  Third
		  Second

		>>> reduce_units(text)
		First
		 Second
		  Third
		 Second
	'''
	old = sorted(set(iterable))
	new = range(0, len(old))
	new = [new_unit * (x + min) for x in new]
	lut = dict(zip(old, new))
	return [lut[x] for x in iterable]

def dict_to_namedtuple(name, dict):
	'''Convert dictionary to named tuple
	'''
	tup = namedtuple(name, dict.keys())
	return tup(*dict.values())

def flatten_nested_dict(item, separator='_', null='null'):
	'''Flatten a given dictionary

	Example:
		>>> flat = flatten_nested_dict(
						{
						 'a': {
							   'b1': {
									  'c': 1},
							   'b2': 0
							}
						 })

		>>> pprint(flat)
		{
			a : null,
			a_b1 : null,
			a_b2 : 0,
			a_b1_c : 1
		}	
	'''
	temp = OrderedDict()
	def _flatten_nested_dict(item, name):
		for key, val in item.iteritems():
			if type(val) is dict and val != {}:
				temp[name + separator + str(key)] = null
				_flatten_nested_dict(val, name + separator + str(key))
			else:
				temp[name + separator + str(key)] = val
	_flatten_nested_dict(item, '__null__')
	output = OrderedDict()
	header = 8 + len(separator)
	for key, value in temp.iteritems():
		output[key[header:]] = value
	return output

def nested_dict_to_matrix(item, justify='left'):
	'''Convert nested dictionary into matrix

	Example:
		>>> item
		{'a': {
			   'b1': {
					  'c': 1},
			   'b2': 0
			}
		}

		>>> nested_dict_to_matrix(item)
		['a', '-->', '-->']
		['a', 'b1', '-->']
		['a', 'b1', 'c']
		['a', 'b2', '-->']
	'''
	matrix = flatten_nested_dict(item, separator='__null__')
	matrix = [x.split('__null__') for x in matrix.keys()]
	max_ = 0
	for item in matrix:
		if len(item) > max_:
			max_ = len(item)
	for item in matrix:
		while len(item) < max_:
			if justify == 'right':
				item.insert(0, '-->')
			else:
				item.append('-->')

	return matrix

def nested_dict_to_index(item, justify='left'):
	'''Convert a nested dictionary to a MultiIndex object
	'''
	index = nested_dict_to_matrix(item, justify=justify)
	index = DataFrame(index).transpose().values.tolist()
	return index

def matrix_to_nested_dict(matrix):
	'''Converts a matrix to a nested dictionary

	Example:
		>>> item
		[ ['a', 'b1', 'c', 1],
		  ['a', 'b2', 0] ]

		>>> nested_dict_to_matrix(item)
		{'a': {
			   'b1': {
					  'c': 1},
			   'b2': 0
			}
		}	
	'''
	output = {}
	for row in matrix:
		keys = row[0:-1]
		value = row[-1]

		cursor = output
		for key in keys[0:-1]:
			if key not in cursor:
				cursor[key] = {}
				cursor = cursor[key]
			else:
				cursor = cursor[key]
		cursor[keys[-1]] = value
	return output

def interpret_nested_dict(item, predicate):
	'''Interpret leaf nodes (values) of a dictionary according to function

	Example:
		>>> x = {
				'a': {
					'b1': {
						   'c': 'convert_me'
					},
					'b2': 'leave_me_alone'
				}
			}

		>>> interpret_nested_dict(x, lambda x: 'NEW_VALUE' if x == 'convert_me' else x)
			x = {
			'a': {
				'b1': {
					   'c': 'NEW_VALUE'
				},
				'b2': 'leave_me_alone'
			}
		}
	'''
	def _interpret_nested_dict(item, cursor):
		for key, val in item.iteritems():
			if type(val) is dict and val != {}:
				cursor[key] = _interpret_nested_dict(val, val)
			else:
				cursor[key] = predicate(val)
		return cursor
	return _interpret_nested_dict(item, {})

def to_inverted_index(item, key):
	'''Converts item into inverted index

	Example:
		>>> employee = {'employee': {'name': 'alex', 'id': 123}}
		>>> to_inverted_index(employee, ['employee', 'name'])
		{'alex': {'employee': {'id': 123, 'name': 'alex'}}}

		>>> employees
		[ {'employee': {'name': 'alex', 'id': 123}},
		  {'employee': {'name': 'janus', 'id': 456}},
		  {'employee': {'name': 'janus', 'id': 456}}, # duplicate record
		  {'employee': {'name': 'atticus', 'id': 789}} ]

		>>> to_inverted_index(employees, ['employee', 'id'])
		{'123': {'employee': {'id': 123, 'name': 'alex'}},
		 '456': {'employee': {'id': 456, 'name': 'janus'}},
		 '789': {'employee': {'id': 789, 'name': 'atticus'}}}
	'''
	def _to_inverted_index(item, key):
		output = {}
		val = item
		if key.__class__.__name__ != 'list':
			val = item[key]
		else:
			for k in key:
				val = val[k]
		output[str(val)] = item
		return output

	if item.__class__.__name__ == 'list':
		output = {}
		for entry in item:
			output.update( _to_inverted_index(entry, key) )
		return output
	else:
		return _to_inverted_index(item, key)
# ------------------------------------------------------------------------------

def irregular_concat(items, axis=0, ignore_index=True):
	'''Concatenate DataFrames of different dimensions

	Example:
		>>> x = DataFrame(numpy.arange(0, 9).reshape(3, 3))
		>>> y = DataFrame(numpy.arange(100, 116).reshape(4, 4))
		>>> irregular_concat([x, y], axis=1)
			0   1   2    0    1    2    3
		0   0   1   2  100  101  102  103
		1   3   4   5  104  105  106  107
		2   6   7   8  108  109  110  111
		3 NaN NaN NaN  112  113  114  115
	''' 
	max_len = 0
	for item in items:
		if len(item) > max_len:
			max_len = len(item)
	
	for item in items:
		bufr = item.head(1)
		bufr = bufr.apply(lambda x: numpy.nan)
		buf_len = max_len - len(item)
		for i in range(buf_len):
			if ignore_index:
				item.append(bufr, ignore_index=True)
			else:
				item.append(bufr)
	
	data = pandas.concat(items, axis=axis)
	return data

def index_to_matrix(index):
	'''Convert a index to a matrix
	'''
	if index.__class__.__name__ == 'MultiIndex':
		index = [list(x) for x in index]
		index = DataFrame(index)
		index = [x[1].tolist() for x in index.iteritems()]
	else:
		index = [index.tolist()]
	return index

def insert_level(index, item, level=0):
	index = index_to_matrix(index)
	item = [item] * len(index[0])
	index.insert(level, item)
	return index

def combine(items):
	'''Combines multiple DataFrames with hierarchical indexes

		Args:
			items (list): List of DataFrames to be combined.

		Returns:
			DataFrame

		Example:
			>>> joe
									  values
			employee username -->      jabra
					 age      -->         67
					 name     last   abrante
							  first      joe
			id       -->      -->          1

			>>> jane
									  values
			employee username -->    jjoplin
					 age      -->         53
					 name     last    joplin
							  first     jane
			id       -->      -->          3

			>>> combine([joe, jane])
									  values
			employee username -->      jabra
					 age      -->         67
					 name     last   abrante
							  first      joe
			id       -->      -->          1
			employee username -->    jjoplin
					 age      -->         53
					 name     last    joplin
							  first     jane
			id       -->      -->          3
	'''

	items = [x.copy() for x in items]
	max_ = 0
	indexes = []
	for item in items:
		index = index_to_matrix(item.index)
		if max_ < len(index):
			max_ = len(index)
		indexes.append(index)
		
	for i, index in enumerate(indexes):
		top_level = index[0]
		for r in range(max_ - len(index)):
			index.insert(0, top_level)
		items[i].index = index
		
	output = items[0]
	for item in items[1:]:
		output = output.append(item)
	return output
# ------------------------------------------------------------------------------

def double_lut_transform(items, input_lut, output_lut):
	# Check luts and issue warnings/errors if necessary
	if input_lut.keys() != output_lut.keys():
		raise KeyError('input lut keys do not match output lut keys')
	
	input_test = input_lut.values()
	for item in input_lut.values():
		input_test.remove(item)
	if len(input_test) > 0:
		input_test = ', '.join(input_test)
		warnings.warn('input lut has duplicate values: ' + input_test, Warning)
	
	output_test = output_lut.values()
	for item in output_lut.values():
		output_test.remove(item)
	if len(output_test) > 0:
		output_test = ', '.join(output_test)
		warnings.warn('output lut has duplicate values: ' + output_test, Warning)
	# --------------------------------------------------------------------------

	reverse_lut = dict(zip(input_lut.values(), input_lut.keys() ))       
	output = []
	for item in items:
		new_item = item
		if item in reverse_lut.keys():
			new_item = output_lut[reverse_lut[item]]
		output.append(new_item)
	return output

def list_to_lut(items, interchange_lut):
	lut = copy(interchange_lut)
	for i, item in enumerate(items):
		lut[lut.keys()[i]] = item
	return lut
# ------------------------------------------------------------------------------

def plot(frame, embedded_column=None, ax=None, colormap=None, figsize=None, 
		 fontsize=None, grid=None, kind='line', legend=True, loglog=False, 
		 logx=False, logy=False, mark_right=True, rot=None, secondary_y=False,
		 sharex=True, sharey=False, sort_columns=False, stacked=False, style=None,
		 subplots=False, table=False, title=None, use_index=True, x=None, xerr=None,
		 xlabel=None, xlim=None, xticks=None, xtick_labels=None, y=None, yerr=None,
		 ylabel=None, ylim=None, yticks=None, ytick_labels=None, 
		 bbox_to_anchor=(0.99, 0.99), loc=0, borderaxespad=0., **kwds):

	fig = pyplot.figure()
	
	if embedded_column:
		frame = frame[embedded_column]
		frame = frame.apply(DataFrame).tolist()
		frame = pandas.concat(frame, axis=1)
	
	if not style:
		style = ['#5F95DE', '#FFFFFF', '#F77465', '#EDED9F', '#AC92DE', 
				 '#7EC4CF', '#A3C987', '#919191']
		style = style * len(frame)
		
	frame.plot(ax=ax, colormap=colormap, figsize=figsize, fontsize=fontsize, 
			   grid=grid, kind=kind, legend=legend, loglog=loglog, logx=logx,
			   logy=logy, mark_right=mark_right, rot=rot, secondary_y=secondary_y, 
			   sharex=sharex, sharey=sharey, sort_columns=sort_columns,
			   stacked=stacked, style=style, subplots=subplots, table=table,
			   title=title, use_index=use_index, x=x, xerr=xerr, xlim=xlim, 
			   xticks=xticks, y=y, yerr=yerr, ylim=ylim, yticks=yticks, **kwds)
	
	if legend:
		pyplot.legend(bbox_to_anchor=bbox_to_anchor, loc=loc, borderaxespad=borderaxespad)

	plt = pyplot.gca()
	if xtick_labels:
		plt.set_xticklabels(xtick_labels)
	if ytick_labels:
		plt.set_yticklabels(ytick_labels)
	if xlabel:
		plt.axes.set_xlabel(xlabel, size='large')
	if ylabel:
		plt.axes.set_ylabel(ylabel, size='large')
	pyplot.show()
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = [
	'Base',
	'to_type', 'is_iterable', 'make_iterable', 'iprint', 'set_decimal_expansion', 
	'try_', 'round_to', 'eval_', 'bool_test', 'regex_match', 'regex_search', 
	'regex_sub', 'regex_split', 'invert', 'reduce_units', 'dict_to_namedtuple', 
	'flatten_nested_dict', 'nested_dict_to_matrix', 'nested_dict_to_index', 
	'matrix_to_nested_dict', 'interpret_nested_dict', 'to_inverted_index', 
	'irregular_concat',	'index_to_matrix', 'insert_level', 'combine', 
	'double_lut_transform',	'list_to_lut', 'plot'
]

if __name__ == '__main__':
	main()