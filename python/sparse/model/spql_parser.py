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

'''
.. module:: spql_parser
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Sparse Query Langauge parser
	
.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

from pyparsing import printables, nums
from pyparsing import Word, Keyword, Or, Group
from pyparsing import delimitedList, oneOf, OneOrMore, Suppress
from sparse.utilities.utils import Base
# ------------------------------------------------------------------------------

class SpQLParser(Base):
	
	def __init__(self, name=None):
		super(SpQLParser, self).__init__(none=name)
		self._cls = 'SpQLParser'

		all_chars            = printables + ' '
		regex                = Suppress('"') + Word(all_chars, excludeChars=',")') + Suppress('"')
		word                 = Word(printables, excludeChars=',")') 
		number               = Word(nums).setParseAction(lambda s,l,t: float(t[0]))
		item                 = Or([number, word, regex])
		items                = delimitedList(OneOrMore(item))
		fields               = Group(Suppress('(') + items + Suppress(')')).setResultsName('fields')
		values               = Group(Suppress('(') + items + Suppress(')')).setResultsName('values')
		pipe                 = Keyword('|')
		is_                  = oneOf(['is',                               '='], caseless=True).setParseAction(lambda s,l,t: '==')
		isnot                = oneOf(['is not',                 'isnot',  '!'], caseless=True).setParseAction(lambda s,l,t: '!=')
		contains             = oneOf(['contains',                'cont',  '{'], caseless=True).setParseAction(lambda s,l,t: 're.IGNORECASE')
		does_not_contain     = oneOf(['does not contain',     'notcont',  '}'], caseless=True).setParseAction(lambda s,l,t: 'nre.IGNORECASE')
		cs_contains          = oneOf(['cscontains',            'cscont', '{{'], caseless=True).setParseAction(lambda s,l,t: 're')
		cs_does_not_contain  = oneOf(['does not cscontain', 'csnotcont', '}}'], caseless=True).setParseAction(lambda s,l,t: 'nre')
		greater_than         = oneOf(['greater than',              'gt',  '>'], caseless=True).setParseAction(lambda s,l,t: '>')
		less_than            = oneOf(['less than',                 'ls',  '<'], caseless=True).setParseAction(lambda s,l,t: '<')
		operator             = isnot | is_ | cs_contains | cs_does_not_contain | contains | does_not_contain | greater_than | less_than
		operator             = operator.setResultsName('operator')
		self._query          = fields + operator + values
		self._compound_query = delimitedList(Word(all_chars, excludeChars='|'), delim=pipe)

		self._last_query = None

	@property
	def last_query(self):
		return self._last_query

	@property
	def query_stats(self):
		for item in self._last_query:
			for key, val in item.iteritems():
				print '{:>8} : {}'.format(key, val)
			print ''

	def search(self, string):
		result = [self._query.parseString(x).asDict() for x in self._compound_query.parseString(string).asList()]
		for item in result:
			item['fields'] = list(item['fields'])
			item['values'] = list(item['values'])

		self._last_query = result
		return result
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''
	
	import __main__
	help(__main__)

__all__ = ['SpQLParser']

if __name__ == '__main__':
	main()