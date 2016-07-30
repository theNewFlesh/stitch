from __future__ import with_statement, print_function, absolute_import
from itertools import *
from functools import *
from pyparsing import printables, nums
from pyparsing import Word, Keyword, Or, Group
from pyparsing import delimitedList, oneOf, OneOrMore, Suppress
from stitch.core.utils import Base
# ------------------------------------------------------------------------------

'''
.. module:: stitch_parser
	:platform: Unix
	:synopsis: Stitch Query Langauge parser

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class StitchParser(Base):
	'''
	Class for generating queries for which to filter DataFrames

	Attributes:
		last_search (str): Last stitchql query generated. Default: None.
		search_stats(str): Print statistics about the last query made.
	'''

	def __init__(self):
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
		print('---------------------------------')
		print('')
		for i, cq in enumerate(self._last_search):
			print('COMPOUND QUERY: ' + str(i))
			for x, q in enumerate(cq):
				print('  QUERY: ' + str(x))
				for key, val in q.iteritems():
					print('    {:>8} : {}'.format(key, val))
			print('---------------------------------')
			print('')

	def search(self, string):
		'''
		Generate query from string and place it in last_search

		Args:
			string(str): stitchql formatted string to be parsed.

		Returns:
			stitchql query.
		'''
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

__all__ = ['StitchParser']

if __name__ == '__main__':
	main()
