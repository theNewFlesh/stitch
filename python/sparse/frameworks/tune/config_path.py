import os
# ------------------------------------------------------------------------------

'''
.. module:: config_path
    :date: 08.22.2014
    :platform: Unix
    :synopsis: Tune configuration path

.. moduleauthor:: Alex Braun <alexander.g.braun@gmail.com>
'''

CONFIG_PATH = os.path.join(os.getcwd(), 'sparse/frameworks/tune/config')
# ------------------------------------------------------------------------------

def main():
	'''
	Run help if called directly
	'''

	import __main__
	help(__main__)

__all__ = ['CONFIG_PATH']

if __name__ == '__main__':
	main()
