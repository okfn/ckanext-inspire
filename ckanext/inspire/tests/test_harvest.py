from ckan import plugins

from ckan.model import Session,Package
from ckan.tests import BaseCase

from ckanext.harvest.model import (setup as harvest_model_setup,
                                    HarvestSource,HarvestJob,HarvestObject)
from ckanext.harvest.lib import (create_harvest_source, create_harvest_job)

from ckanext.inspire.harvesters import GeminiWafHarvester

from simple_http_server import serve

class TestHarvest(BaseCase):

    @classmethod
    def setup_class(cls):
        harvest_model_setup()
        serve()
        
    @classmethod
    def teardown_class(cls):
        pass

    def test_harvest_basic(self):

        # Create source
        source_fixture = {
            'url': u'http://127.0.0.1:8999/waf/index.html',
            'type': u'gemini-waf'
        }

        source_dict = create_harvest_source(source_fixture)
        source = HarvestSource.get(source_dict['id'])
        assert source

        # Create a job
        job_dict = create_harvest_job(source_dict['id'])
        job = HarvestJob.get(job_dict['id'])
        assert job

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
            

