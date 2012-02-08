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

    def test_harvest_fields(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/single/wms1.xml',
            'type': u'gemini-single'
        }

        source, job = self._create_source_and_job(source_fixture)

        harvester = GeminiDocHarvester()

        # We need to send an actual job, not the dict
        object_ids = harvester.gather_stage(job)

        assert len(object_ids) == 1

        # Fetch stage always returns True for Single Doc harvesters
        assert harvester.fetch_stage(object_ids) == True

        obj = HarvestObject.get(object_ids[0])
        assert obj, obj.content
        assert obj.guid == u'73a2683d-081c-473c-8201-4c6578d9d19a'
        
        harvester.import_stage(obj)
        
        context = {'model':model,'session':Session,'user':u'harvest'}
        package_dict = get_action('package_show_rest')(context,{'id':obj.package_id})

        assert package_dict

        expected = {
            'name': u'woodland-survey-sites',
            'title': u'Woodland Survey Sites',
            'notes': u'Woodland Survey Sites',
            'tags': [u'CEH Biodiversity programme', u'NERC_DDC', u'infoMapAccessService'],
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
            'responsible-party': u'CEH Lancaster (pointOfContact, custodian); Test Organization Name (distributor)',
            # Spatial
            'bbox-east-long': u'1.12',
            'bbox-north-lat': u'58.15',
            'bbox-south-lat': u'50.21',
            'bbox-west-long': u'-6.22',
            'spatial': u'{"type":"Polygon","coordinates":[[[1.12, 50.21],[1.12, 58.15], [-6.22, 58.15], [-6.22, 50.21], [1.12, 50.21]]]}',
            # Other
            'access_constraints': u'["Limitations on public access", "restrictions apply"]',
            'spatial-data-service-type': u'view',
            'spatial-reference-system': u'CRS:84',
            'contact-email': 'enquiries@ceh.ac.uk',
            'dataset-reference-date': '[{"type": "creation", "value": "2007-05-01"}]',
            'frequency-of-update': '',
            'metadata-date': '2011-12-13',
            'metadata-language': 'eng',
            'licence': u'["Reference and PSMA Only", "http://www.barrowbc.gov.uk/giscopyright"]',
            'licence_url': u'http://www.barrowbc.gov.uk/giscopyright',
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
            'resource_locator_function': '', #TODO
            'resource_locator_protocol': '', #TODO
            'resource_type': None,
            'size': None,
            'url': 'http://lasigpublic.nerc-lancaster.ac.uk/arcgis/services/Biodiversity/WoodlandSurvey/MapServer/WMSServer?request=getCapabilities&service=WMS',
            'verified': 'True',
        }

        resource = package_dict['resources'][0]
        for key,value in expected_resource.iteritems():
            if not resource[key] == value:
                raise AssertionError('Unexpected value in resource for %s: %s (was expecting %s)' % \
                    (key, resource[key], value))
        assert datetime.strptime(resource['verified_date'],'%Y-%m-%dT%H:%M:%S.%f').date() == date.today() 


