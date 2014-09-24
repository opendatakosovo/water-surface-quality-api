from flask import Response
from bson import json_util, SON

from wsqa import mongo

from average import AverageMeasurements

import flask_pymongo


class AllAverageStationMeasurements(AverageMeasurements):

    def dispatch_request(self):
        '''Get the average of the measurements
        for a given river.
        '''

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
        pipeline = [group, sort]

        collection = mongo.db.watersurfacequality

        # Execute aggregate query
        station_averages = collection.aggregate(pipeline)

        result = station_averages['result']

        resp = Response(
            response=json_util.dumps(result),
            mimetype='application/json')

        return resp

    def get_sort(self):
        sort = {
            '$sort': SON([
                ('_id.lumiSlug', flask_pymongo.ASCENDING),
                ('_id.kodi', flask_pymongo.ASCENDING)
            ])
        }

        return sort
