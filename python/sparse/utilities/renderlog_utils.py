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
.. module:: renderlog_utils
	:date: 04.13.2014
	:platform: Unix
	:synopsis: RenderLogBackingStore utilities

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import re
import numpy
# ------------------------------------------------------------------------------

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