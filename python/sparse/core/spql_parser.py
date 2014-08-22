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
		super(SpQLParser, self).__init__(name=name)
		self._cls = 'SpQLParser'

		all_chars            = printables + ' '
		regex                = Suppress('"') + Word(all_chars, excludeChars=',")') + Suppress('"')
		word                 = Word(printables, excludeChars=',")')
		float_               = Word(nums + '.' + nums).setParseAction(lambda s,l,t: float(t[0]))
		integer              = Word(nums).setParseAction(lambda s,l,t: int(t[0]))
		number               = Or([float_, integer])
		item                 = Or([number, word, regex])
		items                = delimitedList(OneOrMore(item))
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
	def last_search(self):
		return self._last_search

	@property
	def search_stats(self):
		print '---------------------------------'
		print ''
		for i, cq in enumerate(self._last_search):
			print 'COMPOUND QUERY: ' + str(i)
			for x, q in enumerate(cq):
				print '  QUERY: ' + str(x)
				for key, val in q.iteritems():
					print '    {:>8} : {}'.format(key, val)
			print '---------------------------------'
			print ''

	def search(self, string):
		results = []
		for fragment in self._line.parseString(string):
			compound_query = []
			for q in fragment:
				query = {}
				for key, value in q.asDict().iteritems():
					if key == 'operator':
						query[key] = value
					else:
						query[key] = value.asList()
				compound_query.append(query)
			results.append(compound_query)
		
		self._last_search = results
		return results
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