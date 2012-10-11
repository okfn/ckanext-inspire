CKAN INSPIRE extension
======================

Overview
--------

This extension contains:
 * harvesters which import INSPIRE-style metadata into CKAN.
     * the metadata must be in GEMINI2 format (used by the UK Location Programme)
     * the metadata must be retrieved from a CSW server or file servers
 * a way for a user to view the metadata XML, either as a raw file or styled to view in a web browser.

Harvesters
----------

This extension contains these harvesters for specific formats specified by INSPIRE:
 * GeminiHarvester - CSW servers with support for the GEMINI metadata profile
 * GeminiDocHarvester - An individual GEMINI resource
 * GeminiWafHarvester - An index page with links to GEMINI resources

Each contains code to do the three stages of harvesting:
 * gather_stage - Gathers the Harvest Source which lists the metadata URLs
 * fetch_stage - Fetches all the Gemini metadata
 * import_stage - validates the Gemini, converts it to a CKAN Package and saves it in CKAN
 
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

This code falls under two copyrights, depending on when it was contributed:
* Up to 27/2/12: (c) Copyright 2011-2012 Open Knowledge Foundation
* After 27/2/12: Crown Copyright

All of this code is licensed for reuse under the Open Government Licence 
http://www.nationalarchives.gov.uk/doc/open-government-licence/
