try: from cStringIO import StringIO
except ImportError: from StringIO import StringIO
from pylons import response
from pkg_resources import resource_stream, resource_filename
from lxml import etree
from ckan.model.meta import Session
from ckan.model import Package,PackageExtra
from ckan.lib.base import abort

from ckanext.harvest.model import HarvestObject

from ckan.controllers.api import ApiController as BaseApiController

log = __import__("logging").getLogger(__name__)

class ApiController(BaseApiController):

    def _get_harvest_object(self,id):

        obj = Session.query(HarvestObject) \
                        .filter(HarvestObject.id==id).first()
        return obj

    def display_xml(self,id):
        obj = self._get_harvest_object(id)

        if obj is None:
            abort(404)
        response.content_type = "application/xml"
        response.headers["Content-Length"] = len(obj.content)
        return obj.content

    def display_html(self,id):
        obj = self._get_harvest_object(id)

        if obj is None:
            abort(404)
        ## optimise -- read transform only once and compile rather
        ## than at each request
        with resource_stream("ckanext.inspire",
                             "xml/gemini2-html-stylesheet.xsl") as style:
            style_xml = etree.parse(style)
            transformer = etree.XSLT(style_xml)
        xml = etree.parse(StringIO(obj.content.encode("utf-8")))
        html = transformer(xml)
        return etree.tostring(html, pretty_print=True)

