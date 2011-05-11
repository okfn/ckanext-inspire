'''
Different harvesters for INSPIRE related resources

    - GeminiHarvester - CSW servers with support for the GEMINI metadata profile
    - GeminiDocHarvester - An individual GEMINI resource
    - GeminiWafHarvester - An index page with links to GEMINI resources

TODO: Harvesters for generic INSPIRE CSW servers

'''
from lxml import etree
import urllib2
from urlparse import urlparse
from datetime import datetime

import logging
log = logging.getLogger(__name__)

from pylons import config
from sqlalchemy.exc import InvalidRequestError
from ckan.model import Session, repo, \
                        Package, Resource, PackageExtra, \
                        setup_default_user_roles
from ckan.lib.munge import munge_title_to_name
from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from ckanext.inspire.model import GeminiDocument

from owslib import wms

try:
    from ckanext.spatial.lib import save_extent
    save_extents = True
except ImportError:
    log.error('No spatial support installed -- install ckanext-spatial if you want to support spatial queries')
    save_extents = False

try:
    from ckanext.csw.services import CswService
    from ckanext.csw.validation import Validator
    from owslib.csw import namespaces
except ImportError:
    log.error('No CSW support installed -- install ckanext-csw')
    raise


class InspireHarvester(object):
    csw=None

    validator=None

    force_import = False

    def _is_wms(self,url):
        try:
            s = wms.WebMapService(url)
            return isinstance(s.contents, dict) and s.contents != {}
        except:
            pass
        return False

    def _setup_csw_server(self,url):
        self.csw = CswService(url)

    def _get_validator(self):
        profiles = [
            x.strip() for x in
            config.get(
                'ckan.inspire.validator.profiles',
                'iso19139,gemini2',
            ).split(',')
        ]
        self.validator = Validator(profiles=profiles)


    def _save_gather_error(self,message,job):
        err = HarvestGatherError(message=message,job=job)
        try:
            err.save()
        except InvalidRequestError:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        try:
            err.save()
        except InvalidRequestError,e:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _get_content(self, url):
        try:
            url = url.replace(' ','%20')
            http_response = urllib2.urlopen(url)
            return http_response.read()
        except Exception, e:
            raise e


    # All three harvesters share the same import stage
    def import_stage(self,harvest_object):

        if not harvest_object:
            log.error('No harvest object received')
            return False

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:

            self.import_gemini_object(harvest_object.content)
            return True
        except Exception, e:
            self._save_object_error('Error importing Gemini document: %s' % str(e),harvest_object,'Import')

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

    def write_package_from_gemini_string(self, content):
        '''Create or update a Package based on some content that has
        come from a URL.
        '''
        package = None
        gemini_document = GeminiDocument(content)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']

        # Save the metadata reference date in the Harvest Object
        try:
            metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%d')
        except ValueError:
            try:
                metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%dT%H:%M:%S')
            except:
                raise Exception('Could not extract reference date for GUID %s (%s)' \
                        % (gemini_guid,gemini_values['metadata-date']))

        self.obj.metadata_modified_date = metadata_modified_date
        self.obj.save()

        # Look for previously harvested document matching Gemini GUID
        harvested_objects = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==gemini_guid) \
                            .filter(HarvestObject.package!=None) \
                            .order_by(HarvestObject.metadata_modified_date.desc()).all()

        if len(harvested_objects):
            #SA returns nulls first.
            last_harvested_object = harvested_objects[0]
            for ho in harvested_objects:
                if ho.metadata_modified_date:
                    last_harvested_object = ho
                    break
        else:
            last_harvested_object = None

        if last_harvested_object:
            # We've previously harvested this (i.e. it's an update)
            if last_harvested_object.source.id != self.obj.source.id:
                # A 'user' error: there are two or more sources
                # pointing to the same harvested document
                if self.obj.source.id is None:
                    raise Exception('You cannot have an unsaved job source')

                #TODO: Maybe a Warning?
                if last_harvested_object.source.active:
                    raise Exception(
                        'Another active source %s (publisher %s, user %s) is using metadata GUID %s' % (
                            last_harvested_object.source.url,
                            last_harvested_object.source.publisher_id,
                            last_harvested_object.source.user_id,
                            gemini_guid,
                        ))

            # Use metadata modified date instead of content to determine if the package
            # needs to be updated
            if last_harvested_object.metadata_modified_date is None \
                or last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date \
                or self.force_import:

                if self.force_import:
                    log.info('Import forced for package %s' % gemini_guid)
                else:
                    log.info('Package for %s needs to be created or updated' % gemini_guid)

                package = last_harvested_object.package
            else:
                if last_harvested_object.content != self.obj.content and \
                 last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date:
                    raise Exception('The contents of document with GUID %s changed, but the metadata date has not been updated' % gemini_guid)
                else:
                    # The content hasn't changed, no need to update the package
                    log.info('Document with GUID %s unchanged, skipping...' % (gemini_guid))
                return None
        else:
            log.info('No package with GEMINI guid %s found, let''s create one' % gemini_guid)

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
            'coupled-resource',
        ]:
            extras[name] = gemini_values[name]

        extras['licence'] = gemini_values.get('use-constraints', '')
        if len(extras['licence']):
            license_url_extracted = self._extract_first_license_url(extras['licence'])
            if license_url_extracted:
                extras['licence_url'] = license_url_extracted
 
        extras['access_constraints'] = gemini_values.get('limitations-on-public-access','')
        if gemini_values.has_key('temporal-extent-begin'):
            #gemini_values['temporal-extent-begin'].sort()
            extras['temporal_coverage-from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            #gemini_values['temporal-extent-end'].sort()
            extras['temporal_coverage-to'] = gemini_values['temporal-extent-end']

        #Save responsible organization roles
        parties = {}
        for responsible_party in gemini_values['responsible-organisation']:
            if responsible_party['organisation-name'] in parties:
                if not responsible_party['role'] in parties[responsible_party['organisation-name']]:
                    parties[responsible_party['organisation-name']].append(responsible_party['role'])
            else:
                parties[responsible_party['organisation-name']] = [responsible_party['role']]
        parties_extra = []
        for party_name in parties: 
            parties_extra.append('%s (%s)' % (party_name, ', '.join(parties[party_name])))
        extras['responsible-party'] = '; '.join(parties_extra)
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
            if extras['resource-type'] == 'service':
                _format = 'WMS' if self._is_wms(resource_locator) else 'Unverified'
            else: 
                _format = 'Unverified'

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
            log.info('Created new package ID %s with GEMINI guid %s', package.id, gemini_guid)
        else:
            package = self._create_package_from_data(package_data, package = package)
            log.info('Updated existing package ID %s with existing GEMINI guid %s', package.id, gemini_guid)

        # Set reference to package in the HarvestObject
        self.obj.package = package
        self.obj.save()

        # Save spatial extent
        if package.extras.get('bbox-east-long') and save_extents:
            try:
                save_extent(package)
            except:
                log.error('There was an error saving the package extent. Have you set up the package_extent table in the DB?')
                raise

        assert gemini_guid == package.harvest_objects[0].guid
        return package

    def gen_new_name(self,title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u'%s%%' % name
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

    def _extract_first_license_url(self,licences):
        for licence in licences:
            o = urlparse(licence)
            if o.scheme and o.netloc:
                return licence
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

        for resource_dict in package_data.get('resources', []):
            resource = Resource(**resource_dict)
            package.resources[:] = []
            package.resources.append(resource)

        # Make sure old extras are removed if updating
        if len(package.extras):
            for key in package.extras.keys():
                del package.extras[key]

        for key, value in package_data.get('extras', {}).iteritems():
            extra = PackageExtra(key=key, value=value)
            package._extras[key] = extra

        Session.add(package)
        Session.flush()

        setup_default_user_roles(package, [])

        rev.message = 'Harvester: Created package %s' % package.id

        Session.add(rev)
        Session.commit()

        return package

    def get_gemini_string_and_guid(self,content):
        try:
            xml = etree.fromstring(content)

            # The validator and GeminiDocument don't like the container
            metadata_tag = '{http://www.isotc211.org/2005/gmd}MD_Metadata'
            if xml.tag == metadata_tag:
                gemini_xml = xml
            else:
                gemini_xml = xml.find(metadata_tag)

            if not gemini_xml:
                self._save_gather_error('Content is not a valid Gemini document',self.harvest_job)

            if self.validator is not None:
                valid, messages = self.validator.isvalid(gemini_xml)
                if not valid:
                    self._save_gather_error('Content is not a valid Gemini document %r'%messages,self.harvest_job)

            gemini_string = etree.tostring(gemini_xml)
            gemini_document = GeminiDocument(gemini_string)
            gemini_values = gemini_document.read_values()
            gemini_guid = gemini_values['guid']

            return gemini_string, gemini_guid
        except Exception,e:
            raise e

class GeminiHarvester(InspireHarvester,SingletonPlugin):
    '''
    A Harvester for CSW servers that implement the GEMINI metadata profile
    '''
    implements(IHarvester)

    def get_type(self):
        return 'CSW Server'

    def gather_stage(self,harvest_job):
        log.debug('In GeminiHarvester gather_stage')
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
                try:
                    log.info('Got identifier %s from the CSW', identifier)
                    if identifier in used_identifiers:
                        log.error('CSW identifier %r already used, skipping...' % identifier)
                        continue
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        ## log an error here? happens with the dutch data
                        continue

                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid = identifier, job = harvest_job)
                    obj.save()

                    ids.append(obj.id)
                    used_identifiers.append(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    continue

        except Exception, e:
            self._save_gather_error('Error gathering the identifiers from the CSW server [%r]' % e, harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):
        url = harvest_object.source.url
        # Setup CSW server
        try:
            self._setup_csw_server(url)
        except Exception, e:
            self._save_object_error('Error contacting the CSW server: %s' % e,harvest_object)
            return False


        identifier = harvest_object.guid
        try:
            record = self.csw.getrecordbyid([identifier])
        except Exception, e:
            self._save_object_error('Error getting the CSW record with GUID %s' % identifier,harvest_object)
            return False
            
        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            harvest_object.content = record['xml']
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % (identifier,e),harvest_object)
            return False

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True


class GeminiDocHarvester(InspireHarvester,SingletonPlugin):
    '''
    A Harvester for individual GEMINI documents
    '''

    implements(IHarvester)

    def get_type(self):
        return 'Single Document'

    def gather_stage(self,harvest_job):
        log.debug('In GeminiDocHarvester gather_stage')

        # Get source URL
        url = harvest_job.source.url

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None

        try:
            # We need to extract the guid to pass it to the next stage
            gemini_string, gemini_guid = self.get_gemini_string_and_guid(content)
            
            if gemini_guid:
                # Create a new HarvestObject for this identifier
                # Generally the content will be set in the fetch stage, but as we alredy
                # have it, we might as well save a request
                obj = HarvestObject(guid=gemini_guid,
                                    job=harvest_job,
                                    content=gemini_string)
                obj.save()

                log.info('Got GUID %s' % gemini_guid)
                return [obj.id]
            else:
                self._save_gather_error('Could not get the GUID for source %s' % url, harvest_job)
                return None
        except Exception, e:
            self._save_gather_error('Error parsing the document. Is this a valid Gemini document?: %s [%r]'% (url,e),harvest_job)
            return None


    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True


class GeminiWafHarvester(InspireHarvester,SingletonPlugin):
    '''
    A Harvester for index pages with links to GEMINI documents
    '''

    implements(IHarvester)

    def get_type(self):
        return 'Web Accessible Folder (WAF)'

    def gather_stage(self,harvest_job):
        log.debug('In GeminiWafHarvester gather_stage')

        self.harvest_job = harvest_job

        # Get source URL
        url = harvest_job.source.url

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None

        ids = []
        try:
            for url in self._extract_urls(content,url):
                try:
                    content = self._get_content(url)
                except Exception, e:
                    msg = 'Couldn\'t harvest WAF link: %s: %s' % (url, e)
                    self._save_gather_error(msg,harvest_job)
                    continue
                else:
                    # We need to extract the guid to pass it to the next stage
                    try:
                        gemini_string, gemini_guid = self.get_gemini_string_and_guid(content)
                        if gemini_guid:
                            log.debug('Got GUID %s' % gemini_guid)
                            # Create a new HarvestObject for this identifier
                            # Generally the content will be set in the fetch stage, but as we alredy
                            # have it, we might as well save a request
                            obj = HarvestObject(guid=gemini_guid,
                                                job=harvest_job,
                                                content=gemini_string)
                            obj.save()

                            ids.append(obj.id)


                    except Exception,e:
                        msg = 'Could not get GUID for source %s: %r' % (url,e)
                        self._save_gather_error(msg,harvest_job)
                        continue
        except Exception,e:
            msg = 'Error extracting URLs from %s' % url
            self._save_gather_error(msg,harvest_job)
            return None


        if len(ids) > 0:
            return ids
        else:
            self._save_gather_error('Couldn''t find any links to metadata files',
                                     harvest_job)
            return None

    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True


    def _extract_urls(self, content, base_url):
        '''
        Get the URLs out of a WAF index page
        '''
        try:
            parser = etree.HTMLParser()
            tree = etree.fromstring(content, parser=parser)
        except Exception, inst:
            msg = 'Couldn''t parse content into a tree: %s: %s' \
                  % (inst, content)
            raise Exception(msg)
        urls = []
        for url in tree.xpath('//a/@href'):
            url = url.strip()
            if not url:
                continue
            if '?' in url:
                continue
            if '/' in url:
                continue
            urls.append(url)
        base_url = base_url.rstrip('/').split('/')
        if 'index' in base_url[-1]:
            base_url.pop()
        base_url = '/'.join(base_url)
        base_url += '/'
        return [base_url + i for i in urls]


