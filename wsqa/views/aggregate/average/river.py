from flask import Response
from bson import json_util

from wsqa import mongo

from average import AverageMeasurements


class MeasurmentsForARiver(AverageMeasurements):

    methods = ['GET']

    def dispatch_request(self, river_slug):
        '''Get the average of the measurements
        for a given river.
        :param river_slug: slug value of the name of the river.
        '''

        # Match
        match = self.get_match(river_slug)

        # Group
        group = self.get_group("$stacion.lumi")

        # Execute aggregate query
        average_measurments = mongo.db.watersurfacequality.aggregate([
            match,
            group
        ])

        result = average_measurments['result'][0]

        resp = Response(
            response=json_util.dumps(result),
            mimetype='application/json')

        return resp

    def get_match(self, river_slug):
        '''Build and return the match object to be used in aggregation pipeline.
        :param river_slug: name slug of a river.
        '''

        match = {
            "$match": {
                "stacion.lumi.slug": river_slug
            }
        }

        return match
