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

		all_chars            = printables + ' '
		regex                = Suppress('"') + Word(all_chars, excludeChars=',")') + Suppress('"')
		word                 = Word(printables, excludeChars=',")')
		float_               = Word(nums + '.' + nums).setParseAction(lambda s,l,t: float(t[0]))
		integer              = Word(nums).setParseAction(lambda s,l,t: int(t[0]))
		number               = Or([float_, integer])
		bool_ 				 = Or('True', 'False')
		none 				 = Word('None').setParseAction(lambda s,l,t: None)
		
		left_square_bracket  = '['
		right_square_bracket = ']'
		null_list 			 = left_square_bracket + right_square_bracket
		__list_item          = Or([bool_, number, word, regex, none, null_list])
		__list  			 = Suppress('[') + delimitedList(__list_item, delim=',') + Suppress(']')
		_list_item           = Or([bool_, number, word, regex, none, null_list, __list])
		_list                = delimitedList(OneOrMore(_list_item), delim=',')	

		left_curly_bracket	 = '{'
		right_curly_bracket  = '}'
		null_dict 			 = left_curly_bracket + right_curly_bracket
		__dict_value    	 = Or([bool_, number, word, regex, none, null_dict])
		__dict_item          = Group(word + Suppress(':') + __dict_value).setParseAction(lambda s,l,t: {'key': t[0], 'value': t[1]})
		__dict  			 = Suppress(left_curly_bracket) + delimitedList(__dict_item, delim=',') + Suppress(right_curly_bracket)
		_dict_item           = Or([bool_, number, word, regex, none, '{}', __dict])
		_dict                = Suppress('{') + delimitedList(OneOrMore(_dict_item), delim=',') + Suppress('}')




		fields               = Group(Suppress('(') + items + Suppress(')')).setResultsName('fields')
		values               = Group(Suppress('(') + items + Suppress(')')).setResultsName('values')
		is_                  = oneOf(['is',                                '='], caseless=True).setParseAction(lambda s,l,t: '==')
		isnot                = oneOf(['is not',                 'isnot',  '!='], caseless=True).setParseAction(lambda s,l,t: '!=')
		contains             = oneOf(['contains',                'cont',   '~'], caseless=True).setParseAction(lambda s,l,t: 're.IGNORECASE')
		does_not_contain     = oneOf(['does not contain',     'notcont',  '!~'], caseless=True).setParseAction(lambda s,l,t: 'nre.IGNORECASE')
		cs_contains          = oneOf(['cscontains',            'cscont',  '~~'], caseless=True).setParseAction(lambda s,l,t: 're')
		cs_does_not_contain  = oneOf(['does not cscontain', 'csnotcont', '!~~'], caseless=True).setParseAction(lambda s,l,t: 'nre')
		greater_than         = oneOf(['greater than',              'gt',   '>'], caseless=True).setParseAction(lambda s,l,t: '>')
		greater_than_equal   = oneOf(['greater than equal to',    'gte',  '>='], caseless=True).setParseAction(lambda s,l,t: '>=')
		less_than            = oneOf(['less than',                 'ls',   '<'], caseless=True).setParseAction(lambda s,l,t: '<')
		less_than_equal      = oneOf(['less than equal to',       'lte',  '<='], caseless=True).setParseAction(lambda s,l,t: '<=')
		operator             = isnot | is_ | cs_contains | cs_does_not_contain | contains | does_not_contain | greater_than_equal | greater_than | less_than_equal | less_than
		operator             = operator.setResultsName('operator')
		and_                 = Keyword('&')
		or_                  = Keyword('|')
		query                = Group(fields + operator + values)
		compound_query       = Group(delimitedList(query, delim=and_))
		fragment             = OneOrMore(compound_query)
		self._line           = delimitedList(fragment, delim=or_)		

		self._last_search = None

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

	def parse_command(self, command):
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
