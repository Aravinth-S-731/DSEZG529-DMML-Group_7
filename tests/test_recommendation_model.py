import tempfile
import unittest
from pathlib import Path

from recommendation_model.inference import load_model, recommend, save_model
from recommendation_model.train import _evaluation_targets, _fit_item_item_model


class RecommendationModelTests(unittest.TestCase):
    def test_item_cosine_ranks_co_purchased_items_and_excludes_history(self):
        model = _fit_item_item_model(
            {
                "customer-1": {"A", "B"},
                "customer-2": {"A", "B"},
                "customer-3": {"A", "C"},
            },
            max_neighbors=2,
            training_event_count=5,
            training_through="2011-01-01T00:00:00",
            silver_manifest_sha256="a" * 64,
        )

        results = recommend(model, history=["a"], top_k=3)

        self.assertEqual(["B", "C"], [item["stock_code"] for item in results[:2]])
        self.assertNotIn("A", [item["stock_code"] for item in results])

    def test_model_artifact_round_trip_and_unknown_user_popularity_fallback(self):
        model = _fit_item_item_model(
            {
                "customer-1": {"A", "B"},
                "customer-2": {"A", "B"},
                "customer-3": {"A", "C"},
            },
            max_neighbors=2,
            training_event_count=5,
            training_through="2011-01-01T00:00:00",
            silver_manifest_sha256="b" * 64,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            model_path = Path(temporary_directory) / "model.json.gz"
            save_model(model, model_path)
            loaded_model = load_model(model_path)

        results = recommend(loaded_model, customer_id="unknown", top_k=2)

        self.assertEqual(["A", "B"], [item["stock_code"] for item in results])

    def test_temporal_targets_exclude_repeats_and_separate_cold_start(self):
        targets, counts = _evaluation_targets(
            {
                "customer-1": {"A", "B"},
                "customer-2": {"A", "C"},
            },
            {
                "customer-1": {"A", "C", "X"},
                "customer-2": {"D"},
                "customer-3": {"E"},
            },
        )

        self.assertEqual({"customer-1": {"C"}}, targets)
        self.assertEqual(5, counts["heldout_user_product_pairs"])
        self.assertEqual(1, counts["previously_seen_user_product_pairs_excluded"])
        self.assertEqual(3, counts["new_to_user_test_pairs"])
        self.assertEqual(1, counts["warm_item_test_pairs"])
        self.assertEqual(2, counts["cold_item_test_pairs"])
        self.assertEqual(1, counts["heldout_pairs_without_training_history"])


if __name__ == "__main__":
    unittest.main()
