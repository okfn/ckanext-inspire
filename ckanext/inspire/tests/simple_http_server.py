import os

import SimpleHTTPServer
import SocketServer
from threading import Thread


PORT = 8999

def serve(port=PORT):

    # Make sure we serve from the tests directory
    os.chdir(os.path.dirname(os.path.abspath( __file__ )))

    Handler = SimpleHTTPServer.SimpleHTTPRequestHandler

    httpd = SocketServer.TCPServer(("", PORT), Handler)
    
    print 'Serving test HTTP server at port', PORT

    httpd_thread = Thread(target=httpd.serve_forever)
    httpd_thread.setDaemon(True)
    httpd_thread.start()
