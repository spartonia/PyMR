try:
	from setuptools import setup 
except:
	from distutils.core import setup 

config = {
	'description': 'Map Reduce Project with Python',
	'author': 'Saeed Partonia',
	'url': 'URL to get it at.',
	'downlload_url': 'Where to download it.'
	'author_email': 'Saeed.Partonia@gmail.com',
	'version': '0.1',
	'install_requires': ['MRJob', 'nose'],
	'packages': ['name'],
	'scripts': [],
	'name': 'PyMR'
}

setup(**config)