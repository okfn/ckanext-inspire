from ckan.plugins import implements, IRoutes, SingletonPlugin

class InspireApi(SingletonPlugin):
    implements(IRoutes)
        
    def before_map(self, route_map):
        controller = "ckanext.inspire.controllers.api:ApiController"

        route_map.connect("/api/2/rest/harvestobject/:guid/xml", controller=controller,
                          action="display_xml")
        route_map.connect("/api/2/rest/harvestobject/:guid/html", controller=controller,
                          action="display_html")


        return route_map

    def after_map(self, route_map):
        return route_map
