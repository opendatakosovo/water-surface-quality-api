from flask.views import View
from flask import Response
from bson import json_util

from wsqa import mongo

import flask_pymongo


class AllAverageStationMeasurements(View):

    methods = ['GET']

    def dispatch_request(self):
        '''Get the average of the measurements
        for a given river.
        '''

        # Group
        group = self.get_group()

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
            "$sort": {
                "_id": flask_pymongo.ASCENDING
            }
        }

        return sort

    def get_group(self):
        ''' Build and return the group object to be used
        in aggregation pipeline.
        '''

        group = {
            "$group": {
                "_id": "$stacion.kodi",
                "temperaturaUjit": {
                    "$avg": "$temperaturaUjit.vlere"
                },
                "temperaturaAjrit": {
                    "$avg": "$temperaturaAjrit.vlere"
                },
                "turbullira": {
                    "$avg": "$turbullira.vlere"
                },
                "perqueshmeriaElektrike": {
                    "$avg": "$perqueshmeriaElektrike.vlere"
                },
                "materietTretshmeNeUje": {
                    "$avg": "$materietTretshmeNeUje.vlere"
                },
                "perqendrimiJonitHidrogjen": {
                    "$avg": "$perqendrimiJonitHidrogjen.vlere"
                },
                "oksigjeniTretur": {
                    "$avg": "$oksigjeniTretur.vlere"
                },
                "ngopshmeriaMeOksigjen": {
                    "$avg": "$ngopshmeriaMeOksigjen.vlere"
                },
                "shpenzimiKimikOksigjenit": {
                    "$avg": "$shpenzimiKimikOksigjenit.vlere"
                },
                "shpenzimiKimikOksigjenitMeDikromat": {
                    "$avg": "$shpenzimiKimikOksigjenitMeDikromat.vlere"
                },
                "shpenzimiBiokimikOksigjenit": {
                    "$avg": "$shpenzimiBiokimikOksigjenit.vlere"
                },
                "karboniOrganikTotal": {
                    "$avg": "$karboniOrganikTotal.vlere"
                },
                "meterietTotaleTeSuspenduara": {
                    "$avg": "$meterietTotaleTeSuspenduara.vlere"
                },
                "detergjentet": {
                    "$avg": "$detergjentet.vlere"
                },
                "joniNitratet": {
                    "$avg": "$joniNitratet.vlere"
                },
                "azotiNitrateve": {
                    "$avg": "$azotiNitrateve.vlere"
                },
                "joniNitrit": {
                    "$avg": "$joniNitrit.vlere"
                },
                "azotiNitriteve": {
                    "$avg": "$azotiNitriteve.vlere"
                },
                "joniAmonium": {
                    "$avg": "$joniAmonium.vlere"
                },
                "azotiAmoniumit": {
                    "$avg": "$azotiAmoniumit.vlere"
                },
                "azotiTotalInorganik": {
                    "$avg": "$azotiTotalInorganik.vlere"
                },
                "amoniumiPajonizuar": {
                    "$avg": "$amoniumiPajonizuar.vlere"
                },
                "azotiAmoniumitTePajonizuar": {
                    "$avg": "$azotiAmoniumitTePajonizuar.vlere"
                },
                "azotiTotalOrganikinorganik": {
                    "$avg": "$azotiTotalOrganikinorganik.vlere"
                },
                "azotiTotalOrganik": {
                    "$avg": "$azotiTotalOrganik.vlere"
                },
                "ortofosfatet": {
                    "$avg": "$ortofosfatet.vlere"
                },
                "fosforiOrtofosfateve": {
                    "$avg": "$fosforiOrtofosfateve.vlere"
                },
                "fosforiTotalPoliorto": {
                    "$avg": "$fosforiTotalPoliorto.vlere"
                },
                "joniSulfat": {
                    "$avg": "$joniSulfat.vlere"
                },
                "fortesiaPergjithshme": {
                    "$avg": "$fortesiaPergjithshme.vlere"
                },
                "fortesiaKalciumit": {
                    "$avg": "$fortesiaKalciumit.vlere"
                },
                "fortesiaMagnezit": {
                    "$avg": "$fortesiaMagnezit.vlere"
                },
                "jonetKalciumit": {
                    "$avg": "$jonetKalciumit.vlere"
                },
                "jonetMagnezit": {
                    "$avg": "$jonetMagnezit.vlere"
                },
                "palkaliteti": {
                    "$avg": "$palkaliteti.vlere"
                },
                "malkaliteti": {
                    "$avg": "$malkaliteti.vlere"
                },
                "alkalitetiTotal": {
                    "$avg": "$alkalitetiTotal.vlere"
                },
                "bikarbonatet": {
                    "$avg": "$bikarbonatet.vlere"
                },
                "kloriLire": {
                    "$avg": "$kloriLire.vlere"
                },
                "kloruret": {
                    "$avg": "$kloruret.vlere"
                },
                "silikatet": {
                    "$avg": "$silikatet.vlere"
                },
                "siliciNeSilikate": {
                    "$avg": "$siliciNeSilikate.vlere"
                },
                "klorofil": {
                    "$avg": "$klorofil.vlere"
                },
                "fenolet": {
                    "$avg": "$fenolet.vlere"
                }
            }
        }

        return group
