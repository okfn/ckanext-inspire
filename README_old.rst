CKAN INSPIRE extension
======================

.. warning::
   This module is DEPRECATED as of 19/10/12. It has been merged into https://github.com/okfn/ckanext-spatial


Overview
--------

This extension contains:
 * harvesters which import INSPIRE-style metadata into CKAN.
     * the metadata must be in GEMINI2 format (used by the UK Location Programme)
     * the metadata must be retrieved from a CSW server or file servers
 * validators for INSPIRE/GEMINI2 metadata, used by the harvesters
 * a way for a user to view the metadata XML, either as a raw file or styled to view in a web browser.

Harvesters
----------

This extension contains these harvesters for harvesting GEMINI2 metadata from three types of server:
 * GeminiCswHarvester - CSW servers with support for the GEMINI metadata profile
 * GeminiDocHarvester - An individual GEMINI resource
 * GeminiWafHarvester - An index page with links to GEMINI resources

The GEMINI-specific parts of the code are restricted to the fields imported into CKAN, so it would be relatively simple to generalise these to other INSPIRE profiles.

Each contains code to do the three stages of harvesting:
 * gather_stage - Submits a request to Harvest Sources and assembles a list of all the metadata URLs (since each CSW record can recursively refer to more records?). Some processing of the XML or validation may occur.
 * fetch_stage - Fetches all the Gemini metadata
 * import_stage - validates all the Gemini, converts it to a CKAN Package and saves it in CKAN

You must specify which validators to use in the configuration of ``ckan.inspire.validator.profiles`` - see below.

Controllers
-----------

(Enabled with the ``ckan.plugins = inspire_api``)

To view the harvest objects in the web interface, these controller locations are added:

/api/2/rest/harvestobject/<id>/xml

/api/2/rest/harvestobject/<id>/html


Install & Configuration
-----------------------

To install this extension's code into your pyenv::

 pip install -e git+https://github.com/okfn/ckanext-inspire#egg=ckanext-inspire

You also need the dependencies::

 pip install -r pip-requirements.txt

To enable it, in your CKAN config add to ckan.plugins items, as follows::

 ckan.plugins = inspire_api gemini_harvester gemini_doc_harvester gemini_waf_harvester

To change the validation profiles, set this option in your CKAN config::

 ckan.inspire.validator.profiles = iso19139,gemini2,constraints

Licence
-------

This code falls under different copyrights, depending on when it was contributed or where it is:
* Up to 27/2/12: (c) Copyright 2011-2012 Open Knowledge Foundation
* After 27/2/12: Crown Copyright
* In the ckanext/inspire/xml directory: copyright messages are held in the files themselves

All of this code is licensed for reuse under the Open Government Licence 
http://www.nationalarchives.gov.uk/doc/open-government-licence/
