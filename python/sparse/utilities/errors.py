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
.. module:: errors
	:date: 12.29.2013
	:platform: Unix
	:synopsis: Library of error messages

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import re
# ------------------------------------------------------------------------------

class NotFound(Exception):
	def __init__(self, value):
		self._value = value
	def __str__(self):
		return repr(self._value)

class BadArgument(Exception):
	def __init__(self, value):
		self._value = value
	def __str__(self):
		return repr(self._value)

class OperatorError(Exception):
	def __init__(self, value):
		self._value = value
	def __str__(self):
		return repr(self._value)

class MissingKeywordArgument(Exception):
    def __init__(self, message):
        self._message = message
    def __str__(self):
        return self._message

class EmptyFunction(Exception):
    def __init__(self, message):
        self._message = message
    def __str__(self):
        return self._message

def _checkKwargs(self, *kwargs):
    defaultRE = re.compile('~!')
    for kwarg in kwargs:
        if defaultRE.match(str(kwarg)):
            raise MissingKeywordArgument(kwarg[2:])
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)
# ------------------------------------------------------------------------------

__all__ = ['NotFound', 'BadArgument', 'OperatorError', 'MissingKeywordArgument', '_checkKwargs']

if __name__ == '__main__':
	main()