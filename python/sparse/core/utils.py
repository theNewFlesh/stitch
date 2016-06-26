import warnings
import re
from copy import copy, deepcopy
from decimal import Decimal
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from collections import OrderedDict, namedtuple
# ------------------------------------------------------------------------------

'''
.. module:: utils
	:date: 04.13.2014
	:platform: Unix
	:synopsis: General Python utilities

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class Base(object):
	'''Generic base class'''
	def _print_public(self):
		'''Pretty print public methods and attributes'''
		nonPublicRE = re.compile('^_')
		for item in dir(self):
			found = nonPublicRE.match(item)
			if not found:
				print item

	def _print_semiprivate(self):
		'''Pretty print semi-private methods and attributes'''
		semiPrivateRE = re.compile('^_[^_]+')
		for item in dir(self):
			found = semiPrivateRE.match(item)
			if found:
				print item

	def _print_private(self):
		'''Pretty print private methods and attributes'''
		privateRE = re.compile('^__')
		for item in dir(self):
			found = privateRE.match(item)
			if found:
				print item
# ------------------------------------------------------------------------------

def as_type(item, dtype):
	'''Convert item to given type'''
	try:
		return dtype(item)
	except ValueError:
		return item

def as_iterable(item):
	'''Return item in a list if it is not already iterable'''
	if is_iterable(item):
		return item
	else:
		return [item]

def is_iterable(item):
	'''Determine if item is iterable'''
	try:
		result = iter(item)
		return True
	except TypeError:
		return False

def is_dictlike(item):
	'''Determine if an item id dictlike'''
	return item.__class__.__name__ in ['dict', 'OrderedDict']

def is_listlike(item):
	'''Determine if an item id listlike'''
	return item.__class__.__name__ in ['list', 'tuple', 'set']

def is_dict_matrix(item):
	'''Determine if an item is an iterable of dicts'''
	if is_listlike(item):
		if len(item) > 0:
			if is_dictlike(item[0]):
				return True
	return False
# ------------------------------------------------------------------------------

def set_decimal_expansion(item, expansion):
	'''Truncates a float item at specified number of digits after the decimal'''
	return int(item * 10 ** expansion) / float(10 ** expansion)

def round_to(item, order):
	'''Rounds a given number to a given order of magnitudes after the decimal'''
	return round(Decimal(item), order)

def try_(item, func):
	'''Wraps a supplied function in a try block'''
	try:
		return func(item)
	except:
		return item

def eval_(item):
	'''Evaluates item as expression'''
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

def bool_test(item, operator, values):
	'''Perform a boolean operation between an item and a given set of values'''
	ops = { '==': _eq,
			'!=': _ne,
			'<': _lt,
			'<=': _lte,
			'>': _gt,
			'>=': _gte,
			're': _re,
			're.IGNORECASE': _reig,
			'nre': _nre,
			'nre.IGNORECASE': _nreig
	}
	op = ops[operator]
	values = as_iterable(values)
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
	'''Convert a nested dictionary to a MultiIndex object'''
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

def recurse(data, nondict_func=lambda store, key, val: val,
				  dict_func=lambda store, key, val: val,
				  key_func=lambda key: key,
				  store={}):
	def _recurse(data, store):
		if not is_dictlike(data):
			return data
		for key, val in data.iteritems():
			if is_dictlike(val):
				store[key_func(key)] = _recurse(dict_func(store, key, val), {})
			else:
				store[key_func(key)] = nondict_func(store, key, val)
		return store
	return _recurse(data, store)

def merge(data, store, merge_func=lambda store, key, val: [val, store[key]] ):
	def _merge(data, store):
		if not is_dictlike(data):
			return store
		for key, val in data.iteritems():
			if store.has_key(key):
				if not isinstance(val, dict) and not isinstance(store[key], dict):
					store[key] = merge_func(store, key, val)
				else:
					store[key] = _merge(val, store[key])
			else:
				store[key] = val
		return store
	return _merge(deepcopy(data), deepcopy(store))

def merge_list_dicts(source, target, source_key, target_key, remove_key=False):
	def _list_dict_to_dict(items, key, remove_key=False):
		output = {}
		for item in items:
			value = {}
			new_key = str(item[key])
			if remove_key:
				del item[key]
			value[new_key] = item

			out_key = value.keys()[0]
			out_value = value[out_key]
			output[out_key] = out_value
		return output

	source = _list_dict_to_dict(source, source_key, remove_key=remove_key)
	target = _list_dict_to_dict(target, target_key, remove_key=remove_key)
	output = []
	for key, value in source.iteritems():
		row = {}
		for k, v in value.iteritems():
			row[source_key + '_' + str(k)] = v
		for k, v in target[key].iteritems():
			row[target_key + '_' + str(k)] = v
		output.append(row)
	return output

def flatten_list(item):
	store = []
	def _flatten(items):
		if not isinstance(items, list):
			store.append(items)
		for item in items:
			if isinstance(item, list):
				item = _flatten(item)
			else:
				store.append(item)
		return store
	return _flatten(item)

def as_prototype(items):
	'''Converts items to a prototypical dictionary

	Example:
		>>> people
		[{'first': 'tom',    'last': 'flately'},
		 {'first': 'dick',   'last': 'schmidt'},
		 {'first': 'harry',  'last': 'schmidt'}]

		# >>> as_prototype(people)
		# { last : ['flately', 'schmidt', 'schmidt']
		#   first : ['tom', 'dick', 'harry'] }

		>>> as_prototype(people)
		{ last : [],
		  first : [] }
	'''

	prototype = {}
	for item in items:
		prototype = merge(prototype, item)
	prototype = merge(prototype, prototype, lambda store, key, val: [])
	for item in items:
		prototype = merge(prototype, item)
	prototype = recurse(prototype, lambda store, key, val: flatten_list(val))
	return prototype


def as_inverted_dict(item, key, prototype=True):
	'''Converts item into inverted index

	Example:
		>>> employee = {'employee': {'name': 'alex', 'id': 123}}
		>>> as_inverted_dict(employee, ['employee', 'name'])
		{'alex': {'employee': {'id': 123, 'name': 'alex'}}}

		>>> employees
		[ {'employee': {'name': 'alex', 'id': 123}},
		  {'employee': {'name': 'janus', 'id': 456}},
		  {'employee': {'name': 'janus', 'id': 456}}, # duplicate record
		  {'employee': {'name': 'atticus', 'id': 789}} ]

		>>> as_inverted_dict(employees, ['employee', 'id'])
		{'123': {'employee': {'id': 123, 'name': 'alex'}},
		 '456': {'employee': {'id': 456, 'name': 'janus'}},
		 '789': {'employee': {'id': 789, 'name': 'atticus'}}}
	'''
	def _as_inverted_dict(item, key):
		output = {}
		val = item
		if key.__class__.__name__ != 'list':
			val = item[key]
		else:
			for k in key:
				val = val[k]
		output[str(val)] = item
		return output

	if is_listlike(item):
		if prototype:
			output = [_as_inverted_dict(x, key) for x in item]
			return as_prototype(output)
		else:
			output = {}
			for entry in item:
				output.update( _as_inverted_dict(entry, key) )
			return output
	else:
		return _as_inverted_dict(item, key)
# ------------------------------------------------------------------------------

def index_to_matrix(index):
	'''Convert a index to a matrix

	Example:
		>>> data.index
		MultiIndex(levels=[[u'A', u'B', u'C'],
						   [u'a', u'b', u'c']],
				   labels=[[0, 1, 2],
						   [0, 1, 2]])

		>>> index_to_matrix(data.index)
		[['A', 'B', 'C']
		 ['a', 'b', 'c']]
	'''
	if index.__class__.__name__ == 'MultiIndex':
		index = [list(x) for x in index]
		index = DataFrame(index)
		index = [x[1].tolist() for x in index.iteritems()]
	else:
		index = [index.tolist()]
	return index
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

def as_snakecase(string):
    output = re.sub('([^_])([A-Z][a-z]+)', r'\1_\2', string)
    output = re.sub('([a-z0-9])([A-Z])', r'\1_\2', output).lower()
    output = re.sub('\.', '_', output)
    output = re.sub(' +', '_', output)
    output = re.sub('__+', '_', output)
    return output

def nan_to_bottom(series):
	'''Pushes all nan elements to the bottom rows of the Series

	Args:
		series (Series): pandas Series object.

	Returns:
		Series with nan elements at the bottom.
	'''
	data = series.dropna()
	buf = [np.nan] * (series.size - data.size)
	data = data.append(Series(buf))
	data = Series(list(data), index=series.index)
	return data

def reduce_units(series, new_unit='-', min=0):
	'''Replaces all units within a series with a set of smaller units

	Args:
		new_unit (str, optional): Replacement character. Default: '-'.
		min (int, optional): Minimum length of units. Default: 0.

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
	old = sorted(series.unique())
	new = range(0, len(old))
	new = [new_unit * (x + min) for x in new]
	lut = dict(zip(old, new))
	data = series.apply(lambda x: lut[x])
	return data
# ------------------------------------------------------------------------------

def main():
	'''Run help if called directly'''

	import __main__
	help(__main__)

__all__ = [
	'Base',
	'as_type',
	'as_iterable',
	'is_iterable',
	'is_dictlike',
	'is_listlike',
	'is_dict_matrix',
	'set_decimal_expansion',
	'round_to',
	'try_',
	'eval_',
	'bool_test',
	'regex_match',
	'regex_search',
	'regex_sub',
	'regex_split',
	'invert',
	'reduce_units',
	'dict_to_namedtuple',
	'flatten_nested_dict',
	'nested_dict_to_matrix',
	'nested_dict_to_index',
	'matrix_to_nested_dict',
	'interpret_nested_dict',
	'recurse',
	'merge',
	'merge_list_dicts',
	'flatten_list',
	'as_prototype',
	'as_inverted_dict',
	'index_to_matrix',
	'double_lut_transform',
	'list_to_lut',
	'as_snakecase',
	'nan_to_bottom',
	'reduce_units'
]

if __name__ == '__main__':
	main()
