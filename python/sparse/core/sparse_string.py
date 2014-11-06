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

The SparseString class is used for parsing strings and generating regular
expressions according to the DTT (determiner, token, terminator) paradigm. 

Date:
	09.06.2014

Platform:
	Unix

Author:
	Alex Braun <ABraunCCS@gmail.com> <http://www.AlexBraunVFX.com>
'''
# ------------------------------------------------------------------------------

import re
from pandas import DataFrame
from sparse.utilities.utils import Base
# ------------------------------------------------------------------------------
class SparseToken(Base):
    def __init__(self, descriptor='token', determiner='', terminator='', 
                 token='.*', greedy=False, ignore_case=False, tokens_only=True,
                 name=None):
    	super(SparseToken, self).__init__(name=name)
    	self._cls = 'SparseToken'

        self._descriptor  = descriptor
        self._determiner  = determiner
        self._terminator  = terminator
        self._token       = token
        self._greedy      = greedy
        self._ignore_case = ignore_case
        self._tokens_only = tokens_only

    @property
    def descriptor(self):
        return self._descriptor
        
    @property
    def token(self):
        token = '(?P<' + self._descriptor + '>' + self._token
        if self._token != '.*':
            token += ')'
            return token
        
        if self._greedy:
            token += ')'
        else:
            token += '?)'
        return token
        
    @property
    def determiner(self):
        if self._tokens_only:
            return self._determiner
        else:
            return '(?P<' + self._descriptor + '_determiner>' + self._determiner + ')'
    
    @property
    def terminator(self):
        if self._tokens_only:
            return self._terminator
        else:
            return '(?P<' + self._descriptor + '_terminator>' + self._terminator + ')'
        
    @property
    def determiner_type(self):
        if self._determiner:
            return 'explicit'
        else:
            return 'implicit'
            
    @property
    def terminator_type(self):
        if self._terminator:
            return 'explicit'
        else:
            return 'implicit'
    
    @property
    def regex(self):
        return self.determiner + self.token + self.terminator
    
    @property
    def greedy(self):
        if self._greedy:
            return 'greedy'
        else:
            return 'not greedy'

    def parse_string(self, string):
        found = None
        if self._ignore_case:
            found = re.search(self.regex, string, flags=re.IGNORECASE)
        else:    
            found = re.search(self.regex, string)
        if found:
            return found.groupdict()
        else:
            return None
# ------------------------------------------------------------------------------
        
class SparseString(Base):
    def __init__(self, tokens, name=None):
    	super(SparseString, self).__init__(name=name)
    	self._cls = 'SparseString'

        self._tokens = tokens
        data = []
        for item in tokens:
            data.append(['determiner', item.determiner_type, item.descriptor, item._determiner, item.determiner])
            data.append(['token',      item.greedy,          item.descriptor, item._token,      item.token])
            data.append(['terminator', item.terminator_type, item.descriptor, item._terminator, item.terminator])
        data = DataFrame(data, columns=['component', 'type', 'descriptor', 'raw', 'regex'])
        self.data = data
    
    def create_regex(self, drop=None):
        data = self.data
        data = data[data['type'] != 'implicit']
        mask = mask_pairs(data['regex'])
        data = data[mask]
        if drop == 'determiner':
            data = data[data['component'] != 'determiner']
        elif drop == 'terminator':
            data = data[data['component'] != 'terminator']
        regex = data['regex'].tolist()
        regex = ''.join(regex)
        return re.compile(regex)
    
    def parse(self, string, drop=None):
        regex = self.create_regex(drop=drop)
        found = regex.search(string)
        output = None
        if found:
            output = found.groupdict()
        return output
    
    def compound_parse(self, string):
        output = {}
        for token in self._tokens:
            found = token.parse_string(string)
            if found:
                for key in found:
                    output[key] = found[key]
        return output

    def test_parse(self, string):
        def iter_print(item):
            for k, v in item.iteritems():
                print '\t{:<20}: {:<20}'.format(k,v)
        
        print 'parse:'
        iter_print(self.parse(string))
        print '\n'
        
        print 'parse drop=determiner:'
        iter_print(self.parse(string, drop='determiner'))
        print '\n'
        
        print 'parse drop=terminator:'
        iter_print(self.parse(string, drop='terminator'))      
        print '\n'
        
        print 'compound parse:'
        iter_print(self.compound_parse(string))
        print '\n'
# ------------------------------------------------------------------------------
       
def mask_pairs(items):
    prev = items[0]
    output = [True]
    for item in items[1:]:
        if item != prev:
            output.append(True)
        else:
            output.append(False)
        prev = item
    return output
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['SparseToken', 'SparseString']

if __name__ == '__main__':
	main()
