from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-inspire',
	version=version,
	description="CKAN extension for INSPIRE related functions, including harvesting",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Open Knowledge Foudation',
	author_email='info@okfn.org',
	url='',
	license='',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.inspire'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		'ckanext-csw',
        'ckanext-harvest'
	],
	entry_points=\
	"""
    [ckan.plugins]
	# Add plugins here
	inspire_api=ckanext.inspire.plugin:InspireApi
	gemini_harvester=ckanext.inspire.harvesters:GeminiHarvester
 	gemini_doc_harvester=ckanext.inspire.harvesters:GeminiDocHarvester
 	gemini_waf_harvester=ckanext.inspire.harvesters:GeminiWafHarvester
	""",
)
