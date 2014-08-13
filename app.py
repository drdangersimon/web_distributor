from flask import Flask, request
from flask.ext.restful import Resource, Api
import resources.querylocaldir as query

app = Flask(__name__)
api = Api(app)


api.add_resource(query.GalDistribute, '/gal')

if __name__ == '__main__':
    app.run(debug=True)
