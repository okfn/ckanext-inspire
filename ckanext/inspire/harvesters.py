from lxml import etree

import logging
log = logging.getLogger(__name__)

from pylons import config

from ckan.model import Session, repo, \
                        Package, Resource, PackageExtra, \
                        setup_default_user_roles
from ckan.lib.munge import munge_title_to_name
from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestGatherError, HarvestObjectError

from ckanext.inspire.model import GeminiDocument


try:
    from ckanext.csw.services import CswService
    from ckanext.csw.validation import Validator
    from owslib.csw import namespaces
except ImportError:
    log.error('No CSW support installed -- install ckanext-csw')
    raise

class GeminiHarvester(SingletonPlugin):

    implements(IHarvester)
    
    csw=None
        
    validator=None

    def _setup_csw_server(self,url):
        self.csw = CswService(url)

    def _get_validator(self):
        profiles = [
            x.strip() for x in
            config.get(
                "ckan.inspire.validator.profiles", 
                "iso19139,gemini2",
            ).split(",")
        ]
        self.validator = Validator(profiles=profiles)
       

    def _save_gather_error(self,message,job):
        err = HarvestGatherError(message=message,job=job)
        err.save()
        raise Exception(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message, \
                job=obj.job, object=obj,stage=stage)
        err.save()
        raise Exception(message)


    def get_type(self):
        return 'Gemini'

    def gather_stage(self,harvest_job):
        log.info('In GeminiHarvester gather_stage')
        # Get source URL
        url = harvest_job.source.url
        
        # Setup CSW server
        try:
            self._setup_csw_server(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e,harvest_job)
            return None


        log.debug('Starting gathering for %s ' % url)
        used_identifiers = []
        ids = []
        try:
            for identifier in self.csw.getidentifiers(page=10):
                log.info('Got identifier %s from the CSW', identifier)
                if identifier in used_identifiers:
                    log.error('CSW identifier %r already used, skipping...' % identifier)
                    continue
                if identifier is None:
                    #self.job.report['errors'].append('CSW returned identifier %r, skipping...' % identifier)
                    log.error('CSW returned identifier %r, skipping...' % identifier)
                    ## log an error here? happens with the dutch data
                    continue

                # Create a new HarvestObject for this identifier
                obj = HarvestObject(guid = identifier, source = harvest_job.source,job = harvest_job)
                obj.save()

                ids.append(obj.id)
                used_identifiers.append(identifier)
        except Exception, e:
            self._save_gather_error('%r'%e.message,job)
       
        return ids

    def fetch_stage(self,harvest_object):
        url = harvest_object.source.url
        # Setup CSW server
        try:
            self._setup_csw_server(url)
        except Exception, e:
            self._save_object_error('Error contacting the CSW server: %s' % e,harvest_object)
            return None


        identifier = harvest_object.guid 

        record = self.csw.getrecordbyid([identifier])
        if record is None:
            self._save_object_error('Empty record for ID %s' % identifier,harvest_object)
            return False

        # Save the fetch contents in the HarvestObject
        harvest_object.content = record['xml']
        harvest_object.save()

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True
        
    def import_stage(self,harvest_object):
        
        if not harvest_object:
            raise Exception('No harvest object received')
 
        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:

            self.import_gemini_object(harvest_object.content)
            return True
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')



    def import_gemini_object(self, gemini_string):
        try:
            xml = etree.fromstring(gemini_string)

            if not self.validator:
                self._get_validator()

            if self.validator is not None:
                valid, messages = self.validator.isvalid(xml)
                if not valid:
                    self._save_object_error('Content is not a valid Gemini document %r'%messages,self.obj,'Import')

            unicode_gemini_string = etree.tostring(xml, encoding=unicode, pretty_print=True)

            package = self.write_package_from_gemini_string(unicode_gemini_string)

        except Exception, e:
            raise
        else:
            pass
            #TODO: Should we keep this somewhere?
#           if package:
#                self.job.report['added'].append(package.name)

    def write_package_from_gemini_string(self, content):
        '''Create or update a Package based on some content that has
        come from a URL.
        '''
        # Look for previously harvested document matching Gemini GUID
        harvested_doc = None
        package = None
        gemini_document = GeminiDocument(content)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']


        harvested_objects = HarvestObject.filter(guid=gemini_guid) \
                            .order_by(HarvestObject.created.desc()).all()

        last_harvested_object = harvested_objects[1] if len(harvested_objects) > 1 else None
            
        # This is not relevant anymore, as new HarvestObjects are created on every fetch
        '''
        if len(harvested_documents) > 1:
            # A programming error; should never happen
            raise Exception(
                "More than one harvested document GUID %s" % gemini_guid
            )
        elif len(harvested_documents) == 1:
        '''
        if last_harvested_object:
            # We've previously harvested this (i.e. it's an update)
            #TODO: can we delete sources?
            '''
            if harvested_doc.source is None:
                # The source has been deleted, we can re-use it
                log.info('This document existed from another source which was deleted, using your document instead')
                harvested_doc.source = self.job.source
                package = harvested_doc.package
                harvested_doc.save()
                package.save()
                return None
            '''
            if last_harvested_object.source.id != self.obj.source.id:
                # A 'user' error: there are two or more sources
                # pointing to the same harvested document
                if self.obj.source.id is None:
                    raise Exception('You cannot have an unsaved job source')

                raise Exception(
                    literal("Another source %s (publisher %s, user %s) is using metadata GUID %s" % (
                        last_harvested_object.source.url,
                        last_harvested_object.source.publisher_id,
                        last_harvested_object.source.user_id,
                        gemini_guid,
                    ))
                )
            #import pdb
            #pdb.set_trace()

            if last_harvested_object.content == self.obj.content and last_harvested_object.package:
                # The content hasn't changed, no need to update the package, just update
                # the reference to the existing package
                self.obj.package = last_harvested_object.package
                self.obj.save()

                log.info("Document with GUID %s unchanged, skipping..." % (gemini_guid))
                return None

            log.info("Package for %s needs to be created or updated" % gemini_guid)
            package = last_harvested_object.package
        else:
            log.info("No package with GEMINI guid %s found, let's create one" % gemini_guid)

        extras = {
            'published_by': int(self.obj.source.publisher_id or 0),
            'INSPIRE': 'True',
        }

        # Just add some of the metadata as extras, not the whole lot
        for name in [
            # Essentials
            'bbox-east-long', 
            'bbox-north-lat', 
            'bbox-south-lat', 
            'bbox-west-long',
            'spatial-reference-system',
            'guid', 
            # Usefuls
            'dataset-reference-date',
            'resource-type',
            'metadata-language', # Language
            'metadata-date', # Released
        ]:
            extras[name] = gemini_values[name]

        extras['constraint'] = '; '.join(gemini_values.get("use-constraints", '')+gemini_values.get("limitations-on-public-access"))
        if gemini_values.has_key('temporal-extent-begin'):
            #gemini_values['temporal-extent-begin'].sort()
            extras['temporal_coverage-from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            #gemini_values['temporal-extent-end'].sort()
            extras['temporal_coverage-to'] = gemini_values['temporal-extent-end']
        package_data = {
            'title': gemini_values['title'],
            'notes': gemini_values['abstract'],
            'extras': extras,
            'tags': gemini_values['tags'],
        }
        if package is None or package.title != gemini_values['title']:
            name = self.gen_new_name(gemini_values['title'])
            if not name:
                name = self.gen_new_name(str(gemini_guid))
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_data['name'] = name
        resource_locator = gemini_values.get('resource-locator', []) and gemini_values['resource-locator'][0].get('url') or ''

        if resource_locator:
            # TODO: Are we sure that all services are WMS?
            _format = 'WMS' if extras['resource-type'] == 'service' else 'Unverified'
            package_data['resources'] = [
                {
                    'url': resource_locator,
                    'description': 'Resource locator',
                    'format': _format,
                },
            ]

        if package == None:
            # Create new package from data.
            package = self._create_package_from_data(package_data)
            log.info("Created new package ID %s with GEMINI guid %s", package.id, gemini_guid)
        else:
            package = self._create_package_from_data(package_data, package = package)
            log.info("Updated existing package ID %s with existing GEMINI guid %s", package.id, gemini_guid)

        # Set reference to package in the HarvestObject
        self.obj.package = package
        self.obj.save()

        assert gemini_guid == package.harvest_objects[0].guid
        return package

    def gen_new_name(self,title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u"%s%%" % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    def _create_package_from_data(self, package_data, package = None):
        ''' 
        {'extras': {'INSPIRE': 'True',
                    'bbox-east-long': '-3.12442',
                    'bbox-north-lat': '54.218407',
                    'bbox-south-lat': '54.039634',
                    'bbox-west-long': '-3.32485',
                    'constraint': 'conditions unknown; (e) intellectual property rights;',
                    'dataset-reference-date': [{'type': 'creation',
                                                'value': '2008-10-10'},
                                               {'type': 'revision',
                                                'value': '2009-10-08'}],
                    'guid': '00a743bf-cca4-4c19-a8e5-e64f7edbcadd',
                    'metadata-date': '2009-10-16',
                    'metadata-language': 'eng',
                    'published_by': 0,
                    'resource-type': 'dataset',
                    'spatial-reference-system': '<gmd:MD_ReferenceSystem xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xlink="http://www.w3.org/1999/xlink"><gmd:referenceSystemIdentifier><gmd:RS_Identifier><gmd:code><gco:CharacterString>urn:ogc:def:crs:EPSG::27700</gco:CharacterString></gmd:code></gmd:RS_Identifier></gmd:referenceSystemIdentifier></gmd:MD_ReferenceSystem>',
                    'temporal_coverage-from': '1977-03-10T11:45:30',
                    'temporal_coverage-to': '2005-01-15T09:10:00'},
         'name': 'council-owned-litter-bins',
         'notes': 'Location of Council owned litter bins within Borough.',
         'resources': [{'description': 'Resource locator',
                        'format': 'Unverified',
                        'url': 'http://www.barrowbc.gov.uk'}],
         'tags': ['Utility and governmental services'],
         'title': 'Council Owned Litter Bins'}
        '''

        if not package:
            package = Package()

        rev = repo.new_revision()
        
        relationship_attr = ['extras', 'resources', 'tags']

        package_properties = {}
        for key, value in package_data.iteritems():
            if key not in relationship_attr:
                setattr(package, key, value)

        tags = package_data.get('tags', [])

        for tag in tags:
            package.add_tag_by_name(tag, autoflush=False)
        
        for resource_dict in package_data.get("resources", []):
            resource = Resource(**resource_dict)
            package.resources[:] = []
            package.resources.append(resource)

        for key, value in package_data.get("extras", {}).iteritems():
            extra = PackageExtra(key=key, value=value)
            package._extras[key] = extra

        Session.add(package)
        Session.flush()

        setup_default_user_roles(package, [])

        rev.message = 'Harvester: Created package %s' % package.id

        Session.add(rev)
        Session.commit()

        return package

