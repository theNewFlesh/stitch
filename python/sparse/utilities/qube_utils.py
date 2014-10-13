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
.. module:: qube_utils
	:date: 04.13.2014
	:platform: Unix
	:synopsis: Qube BackingStore utilities

.. moduleauthor:: Alex Braun <ABraunCCS@gmail.com>
'''
# ------------------------------------------------------------------------------

import re
from copy import copy
import numpy
# ------------------------------------------------------------------------------

def flatten_qube_field(database, fields):   
	new_db = []
	for job in database:       
		frames = []
		for field in fields:
			for item in job[field]:
				if type(item) != dict:
					item = eval(str(item))
				temp = {}
			for key, value in item.iteritems():
				temp[field + '_' + key] = value    
			frames.append(temp)

		head = job
		
		for frame in frames:
			new_job = copy(head)
			for key, value in frame.iteritems():
				new_job[key] = value
			new_db.append(new_job)
			
	return new_db

def fix_missing_fields(database, fields):
	for field in fields:
		keys = []
		for job in database:
			try:
				job[field] = job[field]
			except:
				continue
			for items in job[field]:
				if item.__class__.__name__ != 'dict':
					items = eval(str(items))
				for key in items:
					keys.append(key)
		
		replacement = {}
		for key in keys:
			replacement[key] = None
		replacement = [replacement]

		for job in database:
			if not field in job.keys():
				job[field] = replacement

	return database

def get_slots(item):
	slots_re = re.compile('host.processor(\d+)/(\d+)')
	found = slots_re.search(item)
	if found:
		return {'used': int(found.group(1)), 'total': int(found.group(2))}
	else:
		return {'used': None, 'total': None}

def str_to_nan(item):
	if item == u'' or item == '':
		return ''
	else:
		return item

def get_procs(item):
	procs_re = re.compile('processors=(\d+)')
	found = procs_re.search(item)
	if found:
		return int(found.group(1))
	else:
		return ''

def get_plus_procs(item):
	procs_re = re.compile('processors=\d+(.)')
	found = procs_re.search(item)
	if found:
		if found.group(1) == '+':
			return '+'
	return ''

def get_ram(item):
	ram_re = re.compile('memory=(\d+)')
	found = ram_re.search(item)
	if found:
		return int(found.group(1))
	else:
		return ''

def get_plus_ram(item):
	pram_re = re.compile('memory=\d+(.)')
	found = pram_re.search(item)
	if found:
		if found.group(1) == '+':
			return '+'
	return ''

def get_dependency(item):
	dep_re = re.compile('.*(complete|job)- ?(\d+|\w[^-]*)[ -]?')
	found = dep_re.search(item)
	if found:
		return found.group(2)
	else:
		return ''
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = [ 'flatten_qube_field', 'fix_missing_fields', 'get_slots', 'str_to_nan',
			'get_procs', 'get_plus_procs', 'get_ram', 'get_plus_ram', 
			'get_dependency']

if __name__ == '__main__':
	main()