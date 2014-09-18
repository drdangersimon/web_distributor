from tornado import gen
from tornado_json.requesthandlers import APIHandler
from tornado_json import schema
import querylocaldir
import storedata
import getdata


class SpectraManage(APIHandler):
    # When you include extra arguments in the signature of an HTTP
    #   method, Tornado-JSON will generate a route that matches the extra
    #   arguments; here, you can GET /api/greeting/John/Smith and you will
    #   get a response back that says, "Greetings, John Smith!"
    # You can match the regex equivalent of `\w+`.
    @schema.validate(
        output_schema={"type": "string"},
        output_example="Greetings, Named Person!"
    )
    '''    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "body": {"type": "string"},
                "index": {"type": "number"},
            }
        }'''
    def get(self, fname, lname):
        """Greets you."""
        return "Greetings, {} {}!".format(fname, lname)


    def post(self):
        ''' '''
        self.body["title"]
