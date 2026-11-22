import unittest

from recommendation_model.app import _recommendation_rows


class RecommendationAppTests(unittest.TestCase):
    def test_recommendation_rows_show_names_and_plain_language_reasons(self):
        rows = _recommendation_rows(
            [
                {
                    "rank": 1,
                    "stock_code": "22423",
                    "score": 0.42,
                    "training_customer_count": 881,
                },
                {
                    "rank": 2,
                    "stock_code": "85123A",
                    "score": 0.0,
                    "training_customer_count": 856,
                },
            ],
            {
                "22423": "REGENCY CAKESTAND 3 TIER",
                "85123A": "WHITE HANGING HEART T-LIGHT HOLDER",
            },
        )

        self.assertEqual(1, rows[0]["Rank"])
        self.assertEqual(
            "REGENCY CAKESTAND 3 TIER (item code 22423)",
            rows[0]["Product"],
        )
        self.assertEqual(
            "Related to products in this customer's history",
            rows[0]["Why we suggest it"],
        )
        self.assertEqual(
            "Popular with other training customers",
            rows[1]["Why we suggest it"],
        )
        self.assertEqual(881, rows[0]["Training customers who bought it"])


if __name__ == "__main__":
    unittest.main()
