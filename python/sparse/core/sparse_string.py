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

'''The sparse_string module contains the SparseString class.

The SparseString class is used for performing advanced operations on strings.
Such as, parsing a string according to a parse index and then splitting it into
dictionary items.

Date:
	09.06.2014

Platform:
	Unix

Author:
	Alex Braun <ABraunCCS@gmail.com> <http://www.AlexBraunVFX.com>
'''
# ------------------------------------------------------------------------------

from __future__ import with_statement
import warnings
import re
from sparse.utilities.utils import Base
# ------------------------------------------------------------------------------

class SparseString(Base):
	def __init__(self, parse_index, verbose=False, name=None):
		super(SparseString, self).__init__(name=name)
		self._cls = 'SparseString'
		self._verbose = verbose
		self._parse_index = {}
		self._regex_index = {}
		self.set_parse_index(parse_index)

	@property
	def parse_index(self):
		return self._parse_index

	@property
	def regex_index(self):
		return self._regex_index

	def _update(self):
		self._regex_index = {}
		for name, entry in self._parse_index.iteritems():
			regex = entry['regex']
			for key, value in entry['keywords'].iteritems():
				kw_re = re.sub('\(', '(?P<' + key + '>', value)
				regex = re.sub(key, kw_re, regex)
			regex = re.compile(regex)
			self._regex_index[name] = regex

	def set_parse_index(self, parse_index):
		self._parse_index = parse_index
		self._update()
		self.test(verbose=self._verbose)

	def parse(self, string):
		for name, regex in self._regex_index.iteritems():
			found = regex.search(string)
			if found:
				if self._verbose:
					print 'matched:', name
				return found.groupdict()
		return {}

	def test(self, verbose=True):
		# test each entry's example
		for name, entry in self._parse_index.iteritems():
			if verbose:
				print name
			test_re = self._regex_index[name]
			test = test_re.search(entry['example'])
			if test:
				test = test.groupdict()
				if verbose:
					for key, value in test.iteritems():
						print '\t{:<20}: {:<20}'.format(key, value)
			else:
				message = "parse of ['" + name + "']['example'] failed to yield results"
				if verbose:
					print '\tWarning:', message
				warnings.warn(message, Warning)
			if verbose:
				print ''

		# test each entry against all other entry's examples
		examples = []
		for name, entry in self._parse_index.iteritems():
			for key, regex in self._regex_index.iteritems():
				if name != key:
					found = regex.search(entry['example'])
					if found:
						message = "['" + key + "']" + " matched ['" + name + "']['example']" 
						warnings.warn(message, Warning)
					else:
						pass
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseString']

if __name__ == '__main__':
	main()
