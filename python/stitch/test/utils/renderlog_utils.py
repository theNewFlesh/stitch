from __future__ import with_statement, print_function, absolute_import
from itertools import *
from functools import *
import re
import numpy
# ------------------------------------------------------------------------------

'''
.. module:: renderlog_utils
	:platform: Unix
	:synopsis: RenderLogBackingStore utilities

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

def get_tracebacks(item):
	tb_err = re.compile('Traceback.*')
	found = tb_err.match(item)
	if found:
		return found.group(0)
	else:
		return numpy.nan

def get_errors(item, logtype='python'):
	err_re = re.compile('[A-Z].*Error: .*')
	if logtype == 'mental ray':
		err_re = re.compile('// Error: .*')
	found = err_re.match(item)
	if found:
		return found.group(0)
	else:
		return numpy.nan

def get_warnings(item, logtype='python'):
	warn_re = re.compile('.*: Warning: (.*)')
	if logtype == 'mental ray':
		warn_re = re.compile('// Warning: (.*)')
	found = warn_re.search(item)
	if found:
		return found.group(1)
	else:
		return numpy.nan

def get_progress(item, logtype='python'):
	prog_re = re.compile('[PERCENT|Percent|percent|PROGRESS|Progress|progress]?\d+\.?\d*[.;]?')
	if logtype == 'alfred':
		prog_re = re.compile('.*ALF_PROGRESS\D*(\d+)')
	found = prog_re.search(item)
	if found:
		return found.group(1)
	else:
		return numpy.nan

def get_traceback_line(item):
	line_re = re.compile('.*, line (\d+),')
	found = line_re.search(item)
	if found:
		return int(found.group(1))
	else:
		return numpy.nan

def get_traceback_file(item):
	file_re = re.compile('File "(.*)"')
	found = file_re.search(item)
	if found:
		return found.group(1)
	else:
		return numpy.nan
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['get_tracebacks', 'get_errors', 'get_warnings', 'get_progress',
			'get_traceback_line', 'get_traceback_file']

if __name__ == '__main__':
	main()
