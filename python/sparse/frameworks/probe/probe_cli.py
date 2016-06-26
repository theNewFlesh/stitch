import sys
from cmd import Cmd
from sparse.core.errors import *
from sparse.frameworks.probe.probe_api import ProbeAPI
import pandas as pd
pd.options.display.width = 500
pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 30
pd.options.display.expand_frame_repr = False
# ------------------------------------------------------------------------------

'''
.. module:: probe_cli
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Command line interface for the Probe API

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

class ProbeCLI(Cmd):
	def __init__(self, backingstore, updates='automatic', display_fields=[],
				 debug_mode=False, prompt='SpQL>'):
		Cmd.__init__(self)
		self.prompt = prompt
		self._api = ProbeAPI(backingstore, updates=updates)
		self._results = None
		self._display_fields = display_fields
		self._debug_mode = debug_mode

	@property
	def results(self):
		# Overwrite this method for displaying custom data
		return self._results

	def default(self, arg):
		if self._debug_mode:
			self._api.spql_search(arg, display_fields=self._display_fields)
			self._results = pd.read_json(self._api._results, orient='records')
			print self._results
		else:
			try:
				self._api.spql_search(arg, display_fields=self._display_fields)
				self._results = pd.read_json(self._api._results, orient='records')
				print self._results
			except NotFound:
				print 'No results found'
			except:
				print 'Improper query'
	# --------------------------------------------------------------------------

	def help_search(self):
		print ''
		print 'Please enter a spql query.'
		print ''
		print '           query: (<field>, <field> ...) <operator> (<value>, <value> ...)'
		print 'multiple queries: <query> | <query> | <query> ...'
		print '         example: (name) contains (jupiter)'
		print '         example: (name) notcont (scratch, test)'
		print '         example: (priority) < (2501)'
		print '         example: (name) contains (jupiter) | (name) notcont (scratch, test) | (priority) < (2501)'

	def help_field(self):
		print ''
		print 'A field is a known key of a database item.'
		print ''
		print 'For instance, if a database exists as a table, then the column headers'
		print 'would count as fields.  A database of people might have the keys:'
		print 'name, height, weight and age.'
		print ''
		print 'All fields (even single ones), must be surrounded by parenthesis'
		print 'use double quotes ("") to capture spaces and other special characters'

	def help_value(self):
		print ''
		print 'A value is the, often unknown, datum associated with the key of a'
		print 'database item.'
		print ''
		print 'For instance, if a database exists as a table, then the data'
		print 'contained in the body would count as values.  A database of people'
		print 'might have an item with the keys: name, height, weight, age, with'
		print 'values of say Steve, 180", 210lbs, 25.'
		print ''
		print 'All values (even single ones), must be surrounded by parenthesis'
		print 'use double quotes ("") to capture spaces and other special characters'

	def help_operator(self):
		print ''
		print 'An operator is a symbol which denotes an operation to be performed'
		print 'on a given database with the left and right operands (field and values)'
		print 'as its arguments.'
		print ''
		print 'For instance, in arithematic, the addition operator (+) combines'
		print 'its two operands into a single value (2 + 2 = 4).  In SpQL, an'
		print 'operator peforms a test on the values of the fields denoted as'
		print 'the left operands, using the values denoted in the right operands'
		print 'as criteria.  So, "(name) contains (jupiter)" searches all the'
		print 'items in a database whose names contain the string "jupiter".'
		print ''
		print 'Operators include:'
		print '                              is,   =  :  exact match'
		print '               is not,     isnot,  !=  :  does not match'
		print '         greater than,        gt,   >  :  greater than'
		print 'greater than equal to,       gte,  >=  :  greater than or equal to'
		print '            less than,        lt,   <  :  less than'
		print '   less than equal to,       lte,  <=  :  less than or equal to'
		print '             contains,      cont,   ~  :  contains'
		print '     does not contain,   notcont,  !~  :  does not contain'
		print '           cscontains,    cscont,  ~~  :  contains (case sensitive)'
		print '   does not cscontain, csnotcont, !~~  :  does not contain (case sensitive)'

	def help_and(self):
		print ''
		print 'The AND operator (&) is a means of chaining queries together.  It pipes the'
		print 'results of the left query into the right query as its new, smaller'
		print 'database.'

	def help_or(self):
		print ''
		print 'The OR operator (|) is a means of concatenating multiple queries into a'
		print 'single result.  Both operands are executed as independent queries and'
		print 'their results are then merged together with duplicate rows removed.'
	# --------------------------------------------------------------------------

	def do_quit(self, arg):
		sys.exit(1)

	def do_q(self, arg):
		self.do_quit(arg)
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['ProbeCLI']

if __name__ == '__main__':
	main()
