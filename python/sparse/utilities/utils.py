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

import re
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

	def _printPublic(self):
		nonPublicRE = re.compile('^_')
		for item in dir(self):
			found = nonPublicRE.match(item)
			if not found:
				print item

	def _printSemiPrivate(self):
		semiPrivateRE = re.compile('^_[^_]+')
		for item in dir(self):
			found = semiPrivateRE.match(item)
			if found:
				print item

	def _printPrivate(self):
		privateRE = re.compile('^__')
		for item in dir(self):
			found = privateRE.match(item)
			if found:
				print item
# ------------------------------------------------------------------------------

def to_type(item, dtype):
    try:
        return dtype(item)
    except ValueError:
        return item

def is_iterable(item):
	try:
		result = iter(item)
		return True
	except TypeError:
		return False

def make_iterable(item):
	if is_iterable(item):
		return item
	else:
		return [item]

def iprint(iterable):
	for item in iterable:
		print item
# ------------------------------------------------------------------------------

def _eq(item, value):
	return item == value

def _ne(item, value):
	return item != value

def _lt(item, value):
	return item < value

def _gt(item, value):
	return item > value

def _re(item, value):
	found = re.search(str(value), str(item))
	if found:
		return True
	else:
		return False

def _reig(item, value):
	found = re.search(str(value), str(item), re.IGNORECASE)
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
	found = re.search(str(value), str(item), re.IGNORECASE)
	if not found:
		return True
	else:
		return False

OPERATORS = {'==': _eq, '!=': _ne, '<': _lt, '>': _gt, 're': _re,
	   're.IGNORECASE': _reig, 'nre': _nre, 'nre.IGNORECASE': _nreig}

def bool_test(item, operator, values):
	op = OPERATORS[operator]
	values = make_iterable(values)
	for value in values:
		if op(item, value):
			return True
	return False
# ------------------------------------------------------------------------------

def regex_match(pattern, string, group=0, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = regex.match(string)
	if found:
		return found.group(group)
	else:
		return string

def regex_search(pattern, string, group=0, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = regex.search(string)
	if found:
		return found.group(group)
	else:
		return string

def regex_sub(pattern, repl, string, count=0, ignore_case=False):
	if ignore_case:
		return re.sub(pattern, repl, string, count, re.IGNORECASE)
	else:
		return re.sub(pattern, repl, string, count)

def regex_split(pattern, string, ignore_case=False):
	if ignore_case:
		regex = re.compile(pattern, re.IGNORECASE)
	else:
		regex = re.compile(pattern)
	found = regex.search(string)
	if found:
		return list(found.groups())
	else:
		return string
# ------------------------------------------------------------------------------

def invert(iterable):
	patterns = sorted(list(set(iterable)))
	inversion_map = dict(zip(patterns, [x for x in reversed(patterns)]))
	return [inversion_map[x] for x in iterable]

def reduce_units(iterable, new_unit='-', min=0):
    old = sorted(set(iterable))
    new = range(0, len(old))
    new = [new_unit * (x + min) for x in new]
    lut = dict(zip(old, new))
    return [lut[x] for x in iterable]
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['Base', 'to_type', 'is_iterable', 'make_iterable', 'iprint',
		   'bool_test', 'regex_match', 'regex_search', 'regex_sub']

if __name__ == '__main__':
	main()