from flask import Response
from bson import json_util

from wsqa import mongo
from average import AverageMeasurements

import flask_pymongo


class AverageStationMeasurementsForGivenRiver(AverageMeasurements):

    def dispatch_request(self, river_slug):
        '''Get the average of each station measurements
        for a given river.
        :param river_slug: slug value of the name of the river.
        '''

        # Match
        match = self.get_match(river_slug)

        # Group
        id_subdoc = {
            "kodi": "$stacion.kodi",
            "lumiEmri": "$stacion.lumi.emri",
            "lumiSlug": "$stacion.lumi.slug",
            "kordinatat": "$stacion.kordinatat"
        }

        group = self.get_group(id_subdoc)

        # Sort
        sort = self.get_sort()

        # The aggregation pipeline
        pipeline = [match, group, sort]

        # Execute aggregate query
        averages = mongo.db.watersurfacequality.aggregate(pipeline)

        result = averages['result']

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

    def get_sort(self):
        sort = {
            '$sort': {
                '_id.kodi': flask_pymongo.ASCENDING
            }
        }

        return sort
