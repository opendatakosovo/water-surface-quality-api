from flask import current_app, Response, request
from flask.views import View

from bson import json_util, SON
from wsqa import mongo

import flask_pymongo


class Measurements(View):

    methods = ['GET']

    def dispatch_request(self):
        '''Get the measurements
        '''

        pipeline = []

        project = self.get_project()
        pipeline.append(project)

        # Match
        year = request.args.get('year', None)
        month = request.args.get('month', None)

        if year is not None and month is not None:
            try:
                year_int = int(year)
                month_int = int(month)

                match = self.get_match(year_int, month_int)
                pipeline.append(match)

            except ValueError:
                error_msg = "There was an error casting year and/or month URL parameters: (%s, %s)." % (year, month)
                current_app.logger.error(error_msg)

        # Sort
        sort = self.get_sort()
        pipeline.append(sort)

        # Execute aggregate query
        measurments = mongo.db.watersurfacequality.aggregate(pipeline)

        result = measurments['result']

        resp = Response(
            response=json_util.dumps(result),
            mimetype='application/json')

        return resp

    def get_project(self):
        project = {
            "$project": {
                "data": {
                    "viti": {
                        "$year": "$data"
                    },
                    "muji": {
                        "$month": "$data"
                    },
                    "dita": {
                        "$dayOfMonth": "$data"
                    }
                },
                "stacion": 1,
                "temperaturaUjit": 1,
                "temperaturaAjrit": 1,
                "turbullira": 1,
                "perqueshmeriaElektrike": 1,
                "materietTretshmeNeUje": 1,
                "perqendrimiJonitHidrogjen": 1,
                "oksigjeniTretur": 1,
                "ngopshmeriaMeOksigjen": 1,
                "shpenzimiKimikOksigjenit": 1,
                "shpenzimiKimikOksigjenitMeDikromat": 1,
                "shpenzimiBiokimikOksigjenitSHBO5": 1,
                "shpenzimiBiokimikOksigjenitSHBO7": 1,
                "karboniOrganikTotal": 1,
                "meterietTotaleTeSuspenduara": 1,
                "detergjentet": 1,
                "joniNitratet": 1,
                "azotiNitrateve": 1,
                "joniNitrit": 1,
                "azotiNitriteve": 1,
                "joniAmonium": 1,
                "azotiAmoniumit": 1,
                "azotiTotalInorganik": 1,
                "amoniumiPajonizuar": 1,
                "azotiAmoniumitTePajonizuar": 1,
                "azotiTotalOrganikinorganik": 1,
                "azotiTotalOrganik": 1,
                "ortofosfatet": 1,
                "fosforiOrtofosfateve": 1,
                "fosforiTotalPoliorto": 1,
                "joniSulfat": 1,
                "fortesiaPergjithshme": 1,
                "fortesiaKalciumit": 1,
                "fortesiaMagnezit": 1,
                "jonetKalciumit": 1,
                "jonetMagnezit": 1,
                "palkaliteti": 1,
                "malkaliteti": 1,
                "alkalitetiTotal": 1,
                "bikarbonatet": 1,
                "kloriLire": 1,
                "kloruret": 1,
                "silikatet": 1,
                "siliciNeSilikate": 1,
                "klorofil": 1,
                "fenolet": 1
            }
        }

        return project

    def get_match(self, year, month):
        '''Build and return the match object .
        :param year: the sampling year.
        :param month: the sampling month.
        '''
        match = {
            "$match": {
                "data.viti": year,
                "data.muji": month
            }
        }

        return match

    def get_sort(self):
        sort = {
            '$sort': SON([
                ('stacion.lumi.slug', flask_pymongo.ASCENDING),
                ('stacion.kodi', flask_pymongo.ASCENDING)
            ])
        }
        return sort
