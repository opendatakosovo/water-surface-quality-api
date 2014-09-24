from flask.views import View


class AverageMeasurements(View):

    methods = ['GET']

    def get_group(self, id_subdoc):
        ''' Build and return the group object to be used
        in aggregation pipeline.
        :param id_subdoc: the sub document object that define the group id.
        '''

        group = {
            "$group": {
                "_id": id_subdoc,
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
                "shpenzimiBiokimikOksigjenitSHBO5": {
                    "$avg": "$shpenzimiBiokimikOksigjenitSHBO5.vlere"
                },
                "shpenzimiBiokimikOksigjenitSHBO7": {
                    "$avg": "$shpenzimiBiokimikOksigjenitSHBO7.vlere"
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
