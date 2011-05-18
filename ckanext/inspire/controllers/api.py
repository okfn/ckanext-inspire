try: from cStringIO import StringIO
except ImportError: from StringIO import StringIO
from pylons import response
from pkg_resources import resource_stream, resource_filename
from lxml import etree
from ckan.model.meta import Session
from ckan.lib.base import abort

from ckanext.harvest.model import HarvestObject

from ckan.controllers.api import ApiController as BaseApiController

log = __import__("logging").getLogger(__name__)

class ApiController(BaseApiController):

    def _get_harvest_object(self,guid):
        obj = Session.query(HarvestObject) \
                        .filter(HarvestObject.guid==guid) \
                        .filter(HarvestObject.package!=None) \
                        .filter(HarvestObject.metadata_modified_date!=None) \
                        .order_by(HarvestObject.metadata_modified_date.desc()) \
                        .limit(1).first()
        if not obj:
            #Just in case metadata_modified_dates have not been yet set up
            obj = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==guid) \
                            .filter(HarvestObject.package!=None) \
                            .order_by(HarvestObject.gathered.desc()) \
                            .limit(1).first()
        return obj

    def display_xml(self, guid):
        doc = self._get_harvest_object(guid)

        if doc is None:
            abort(404)
        response.content_type = "application/xml"
        response.headers["Content-Length"] = len(doc.content)
        return doc.content

    def display_html(self, guid):
        doc = self._get_harvest_object(guid)

        if doc is None:
            abort(404)
        ## optimise -- read transform only once and compile rather
        ## than at each request
        with resource_stream("ckanext.inspire",
                             "xml/gemini2-html-stylesheet.xsl") as style:
            style_xml = etree.parse(style)
            transformer = etree.XSLT(style_xml)
        more_than_meets_the_eyes = etree.parse(StringIO(doc.content.encode("utf-8")))
        html = transformer(more_than_meets_the_eyes)
        return etree.tostring(html, pretty_print=True)

