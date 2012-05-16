CKAN INSPIRE extension
======================

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

 ckan.plugins = inspire_api, gemini_harvester, gemini_doc_harvester, gemini_waf_harvester

To change the validation profiles, set this option in your CKAN config::

 ckan.inspire.validator.profiles = iso19139, gemini2, constraints
