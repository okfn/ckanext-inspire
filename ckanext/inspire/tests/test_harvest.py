from datetime import datetime,date
from ckan import plugins

from ckan import model
from ckan.model import Session,Package
from ckan.tests import BaseCase

from ckan.logic import get_action


from ckanext.harvest.model import (setup as harvest_model_setup,
                                    HarvestSource,HarvestJob,HarvestObject)
from ckanext.harvest.lib import (create_harvest_source, create_harvest_job)

from ckanext.inspire.harvesters import GeminiHarvester, GeminiDocHarvester, GeminiWafHarvester

from simple_http_server import serve

class TestHarvest(BaseCase):

    @classmethod
    def setup_class(cls):
        harvest_model_setup()
        serve()

    @classmethod
    def teardown_class(cls):
        pass

    def _create_source_and_job(self,source_fixture):

        source_dict = create_harvest_source(source_fixture)
        source = HarvestSource.get(source_dict['id'])
        assert source

        # Create a job
        job_dict = create_harvest_job(source_dict['id'])
        job = HarvestJob.get(job_dict['id'])
        assert job

        return source, job

    def test_harvest_basic(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/waf/index.html',
            'type': u'gemini-waf'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiWafHarvester()

        # We need to send an actual job, not the dict
        object_ids = harvester.gather_stage(job)

        assert len(object_ids) == 2

        # Fetch stage always returns True for Waf harvesters
        assert harvester.fetch_stage(object_ids) == True

        objects = []
        for object_id in object_ids:
            obj = HarvestObject.get(object_id)
            assert obj
            objects.append(obj)
            harvester.import_stage(obj)

        pkgs = Session.query(Package).all()

        assert len(pkgs) == 2

        pkg_ids = [pkg.id for pkg in pkgs]

        for obj in objects:
            assert obj.current == True
            assert obj.package_id in pkg_ids

    def test_harvest_fields_service(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/service1.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        object_ids = harvester.gather_stage(job)
        assert object_ids, len(object_ids) == 1

        # No gather errors
        assert len(job.gather_errors) == 0

        # Fetch stage always returns True for Single Doc harvesters
        assert harvester.fetch_stage(object_ids) == True

        obj = HarvestObject.get(object_ids[0])
        assert obj, obj.content
        assert obj.guid == u'test-service-1'

        harvester.import_stage(obj)

        # No object errors
        assert len(obj.errors) == 0

        context = {'model':model,'session':Session,'user':u'harvest'}
        package_dict = get_action('package_show_rest')(context,{'id':obj.package_id})

        assert package_dict

        expected = {
            'name': u'one-scotland-address-gazetteer-web-map-service-wms',
            'title': u'One Scotland Address Gazetteer Web Map Service (WMS)',
            'tags': [u'Addresses', u'Scottish National Gazetteer'],
            'notes': u'This service displays its contents at larger scale than 1:10000. [edited]',
        }

        for key,value in expected.iteritems():
            if not package_dict[key] == value:
                raise AssertionError('Unexpected value for %s: %s (was expecting %s)' % \
                    (key, package_dict[key], value))

        expected_extras = {
            # Basic
            'harvest_object_id': obj.id,
            'guid': obj.guid,
            'UKLP': u'True',
            'resource-type': u'service',
            'access_constraints': u'["No restriction on public access"]',
            'responsible-party': u'The Improvement Service (resourceProvider)',
            'contact-email': u'OSGCM@improvementservice.org.uk',
            # Spatial
            'bbox-east-long': u'0.5242365625',
            'bbox-north-lat': u'61.0243',
            'bbox-south-lat': u'54.4764484375',
            'bbox-west-long': u'-9.099786875',
            'spatial': u'{"type":"Polygon","coordinates":[[[0.5242365625, 54.4764484375],[0.5242365625, 61.0243], [-9.099786875, 61.0243], [-9.099786875, 54.4764484375], [0.5242365625, 54.4764484375]]]}',
            # Other
            'coupled-resource': u'[{"href": ["http://scotgovsdi.edina.ac.uk/srv/en/csw?service=CSW&request=GetRecordById&version=2.0.2&outputSchema=http://www.isotc211.org/2005/gmd&elementSetName=full&id=250ea276-48e2-4189-8a89-fcc4ca92d652"], "uuid": ["250ea276-48e2-4189-8a89-fcc4ca92d652"], "title": []}]',
            'dataset-reference-date': u'[{"type": "publication", "value": "2011-09-08"}]',
            'frequency-of-update': u'daily',
            'licence': u'["Use of the One Scotland Gazetteer data used by this this service is available to any organisation that is a member of the One Scotland Mapping Agreement. It is not currently commercially available", "http://www.test.gov.uk/licenseurl"]',
            'licence_url': u'http://www.test.gov.uk/licenseurl',
            'metadata-date': u'2011-09-08T16:07:32',
            'metadata-language': u'eng',
            'spatial-data-service-type': u'other',
            'spatial-reference-system': u'OSGB 1936 / British National Grid (EPSG:27700)',
            'temporal_coverage-from': u'["1904-06-16"]',
            'temporal_coverage-to': u'["2004-06-16"]',
        }

        for key,value in expected_extras.iteritems():
            if not key in package_dict['extras']:
                raise AssertionError('Extra %s not present in package' % key)

            if not package_dict['extras'][key] == value:
                raise AssertionError('Unexpected value for extra %s: %s (was expecting %s)' % \
                    (key, package_dict['extras'][key], value))

        expected_resource = {
            'ckan_recommended_wms_preview': 'True',
            'description': 'Link to the GetCapabilities request for this service',
            'format': 'WMS',
            'name': 'Web Map Service (WMS)',
            'resource_locator_function': 'download',
            'resource_locator_protocol': 'OGC:WMS-1.3.0-http-get-capabilities',
            'resource_type': None,
            'size': None,
            'url': u'http://sedsh13.sedsh.gov.uk/ArcGIS/services/OSG/OSG/MapServer/WMSServer?request=GetCapabilities&service=WMS',
            'verified': 'True',
        }

        resource = package_dict['resources'][0]
        for key,value in expected_resource.iteritems():
            if not resource[key] == value:
                raise AssertionError('Unexpected value in resource for %s: %s (was expecting %s)' % \
                    (key, resource[key], value))
        assert datetime.strptime(resource['verified_date'],'%Y-%m-%dT%H:%M:%S.%f').date() == date.today()

    def test_harvest_fields_dataset(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/dataset1.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        object_ids = harvester.gather_stage(job)
        assert object_ids, len(object_ids) == 1

        # No gather errors
        assert len(job.gather_errors) == 0

        # Fetch stage always returns True for Single Doc harvesters
        assert harvester.fetch_stage(object_ids) == True

        obj = HarvestObject.get(object_ids[0])
        assert obj, obj.content
        assert obj.guid == u'test-dataset-1'

        harvester.import_stage(obj)

        # No object errors
        assert len(obj.errors) == 0

        context = {'model':model,'session':Session,'user':u'harvest'}
        package_dict = get_action('package_show_rest')(context,{'id':obj.package_id})

        assert package_dict

        expected = {
            'name': u'country-parks-scotland',
            'title': u'Country Parks (Scotland)',
            'tags': [u'Nature conservation'],
            'notes': u'Parks are set up by Local Authorities to provide open-air recreation facilities close to towns and cities. [edited]'
        }

        for key,value in expected.iteritems():
            if not package_dict[key] == value:
                raise AssertionError('Unexpected value for %s: %s (was expecting %s)' % \
                    (key, package_dict[key], value))

        expected_extras = {
            # Basic
            'harvest_object_id': obj.id,
            'guid': obj.guid,
            'resource-type': u'dataset',
            'responsible-party': u'Scottish Natural Heritage (custodian, distributor)',
            'access_constraints': u'["Copyright Scottish Natural Heritage"]',
            'contact-email': u'data_supply@snh.gov.uk',
            # Spatial
            'bbox-east-long': u'0.205857204',
            'bbox-north-lat': u'61.06066944',
            'bbox-south-lat': u'54.529947158',
            'bbox-west-long': u'-8.97114288',
            'spatial': u'{"type":"Polygon","coordinates":[[[0.205857204, 54.529947158],[0.205857204, 61.06066944], [-8.97114288, 61.06066944], [-8.97114288, 54.529947158], [0.205857204, 54.529947158]]]}',
            # Other
            'coupled-resource': u'[]',
            'dataset-reference-date': u'[{"type": "creation", "value": "2004-02"}, {"type": "revision", "value": "2006-07-03"}]',
            'frequency-of-update': u'irregular',
            'licence': u'["Reference and PSMA Only", "http://www.test.gov.uk/licenseurl"]',
            'licence_url': u'http://www.test.gov.uk/licenseurl',
            'metadata-date': u'2011-09-23T10:06:08',
            'metadata-language': u'eng',
            'spatial-reference-system': u'urn:ogc:def:crs:EPSG::27700',
            'temporal_coverage-from': u'["1998"]',
            'temporal_coverage-to': u'["2010"]',
        }

        for key,value in expected_extras.iteritems():
            if not key in package_dict['extras']:
                raise AssertionError('Extra %s not present in package' % key)

            if not package_dict['extras'][key] == value:
                raise AssertionError('Unexpected value for extra %s: %s (was expecting %s)' % \
                    (key, package_dict['extras'][key], value))

        expected_resource = {
            'description': 'Test Resource Description',
            'format': u'',
            'name': 'Test Resource Name',
            'resource_locator_function': 'download',
            'resource_locator_protocol': 'test-protocol',
            'resource_type': None,
            'size': None,
            'url': u'https://gateway.snh.gov.uk/pls/apex_ddtdb2/f?p=101',
        }

        resource = package_dict['resources'][0]
        for key,value in expected_resource.iteritems():
            if not resource[key] == value:
                raise AssertionError('Unexpected value in resource for %s: %s (was expecting %s)' % \
                    (key, resource[key], value))

    def test_harvest_error_bad_xml(self):
        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/error_bad_xml.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        object_ids = harvester.gather_stage(job)
        assert object_ids is None

        # Check gather errors
        assert len(job.gather_errors) == 1
        assert job.gather_errors[0].harvest_job_id == job.id
        assert 'Error parsing the document' in job.gather_errors[0].message

    def test_harvest_error_404(self):
        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/not_there.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        object_ids = harvester.gather_stage(job)
        assert object_ids is None

        # Check gather errors
        assert len(job.gather_errors) == 1
        assert job.gather_errors[0].harvest_job_id == job.id
        assert 'Unable to get content for URL' in job.gather_errors[0].message

    def test_harvest_error_validation(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/error_validation.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        object_ids = harvester.gather_stage(job)

        # Right now the import process goes ahead even with validation errors
        assert object_ids, len(object_ids) == 1

        # No gather errors
        assert len(job.gather_errors) == 1
        assert job.gather_errors[0].harvest_job_id == job.id

        message = job.gather_errors[0].message

        assert 'Validation error' in message
        assert 'Validating against gemini2 profile failed' in message
        assert 'The metadata point of contact role shall be \'pointOfContact\'' in message
        assert 'One email address shall be provided' in message
        assert 'Service type shall be one of \'discovery\', \'view\', \'download\', \'transformation\', \'invoke\' or \'other\' following INSPIRE generic names' in message
        assert 'Limitations on public access code list value shall be \'otherRestrictions\'' in message
        assert 'One organisation name shall be provided' in message

        # Fetch stage always returns True for Single Doc harvesters
        assert harvester.fetch_stage(object_ids) == True

        obj = HarvestObject.get(object_ids[0])
        assert obj, obj.content
        assert obj.guid == u'test-error-validation-1'

        harvester.import_stage(obj)

        # Check errors
        assert len(obj.errors) == 1

