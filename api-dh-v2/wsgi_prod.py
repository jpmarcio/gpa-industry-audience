from GPA_meudesconto_api import app as application
import utils
import unittest

payload = {"produtos": ["1062850"],
           "precoProduto": 3.99,
           "objetivo": "frequency",
           "mecanica": {"tipoMecanica": "PERCENTUAL",
                        "valorDesconto": 30},
           "grupo": "300330705"}
payload_filtr = {"produtos": ["0251952"],
                 "filtros": {"genero": [],
                            "idade": [],
                            "sensibilidadePreco": None,
                            "lealdadeProduto": 3,
                            "un": ["01010107382"]},
                 "grupo": "750075023"}
global payload, payload_filtr


class TestSequenceFunctions(unittest.TestCase):

    def test_calculo_percentual(self):
        # make sure the shuffled sequence does not lose any elements
        assert_result_expected = (0.5, 0.01)
        self.assertEqual(utils2.calculo_percentual(1, 1), assert_result_expected)

    def test_calculo_estimativo(self):
        assert_result_expected = (72, 173, 403)
        self.assertEqual(utils2.calculo_estimativo(1442, payload, 2), assert_result_expected)

    def test_execute_query(self):
        self.assertEqual(utils2.execute_query("SELECT 0 as zero")[0][0], 0)

    def test_get_audience_from_db(self):
        self.assertEqual(utils2.get_audience_from_db(payload), 1442)
        self.assertEqual(utils2.get_audience_from_db(payload_filtr), 74042)
        try:
            utils2.get_audience_from_db('0')
            assert False
        except TypeError:
            assert True

    def test_select_regio(self):
        self.assertEqual(utils2.select_regions([1]), " AND prsn_address_state_prov_code in ('SP')")

    def test_get_index_info(self):
        self.assertEqual(utils2.get_index_info('0251952', '')[0], '01010101535')

    def test_get_non_un_filters(self):
        self.assertEqual(utils2.get_non_un_filters(payload_filtr), ('', '', ''))
        self.assertEqual(utils2.get_non_un_filters(payload), ('', '', ' AND is_frequency_increase_candidate = True '))


if __name__ == '__main__':
    unittest.main()
    application.run()

