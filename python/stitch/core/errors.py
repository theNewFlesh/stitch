import re
# ------------------------------------------------------------------------------

'''
.. module:: errors
	:platform: Unix
	:synopsis: Library of error messages

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

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

__all__ = ['NotFound', 'BadArgument', 'OperatorError', 'MissingKeywordArgument',
		   '_checkKwargs']

if __name__ == '__main__':
	main()
