import json
import tornado.ioloop
from tornado_json.routes import get_routes
from tornado_json.application import Application
import resources

def main():
    # Pass the web app's package the get_routes and it will generate
    #   routes based on the submodule names and ending with lowercase
    #   request handler name (with 'handler' removed from the end of the
    #   name if it is the name).
    # [("/api/helloworld", helloworld.api.HelloWorldHandler)]
    routes = get_routes(resources)
    print("Routes\n======\n\n" + json.dumps(
        [(url, repr(rh)) for url, rh in routes],
        indent=2)
    )
    # Create the application by passing routes and any settings
    application = Application(routes=routes, settings={})

    # Start the application on port 8888
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
