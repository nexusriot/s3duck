import unittest

from core.listing_cache import ListingCache


class ListingCacheTests(unittest.TestCase):
    @staticmethod
    def payload(count):
        return dict(bucket="bkt", prefix="", items=list(range(count)),
                    truncated=True, promoted={"endpoint_url": "old"})

    def test_evicts_least_recently_used_folder(self):
        cache = ListingCache(max_folders=2)
        cache.put("a", self.payload(1))
        cache.put("b", self.payload(1))
        cache.get("a")
        cache.put("c", self.payload(1))
        self.assertIsNone(cache.get("b"))
        self.assertIsNotNone(cache.get("a"))

    def test_total_item_bound_and_oversized_listing(self):
        cache = ListingCache(max_items=3)
        cache.put("a", self.payload(2))
        cache.put("b", self.payload(2))
        self.assertIsNone(cache.get("a"))
        cache.put("big", self.payload(4))
        self.assertIsNone(cache.get("big"))
        self.assertIsNotNone(cache.get("b"))

    def test_replacement_and_clear_reset_item_count(self):
        cache = ListingCache(max_items=3)
        cache.put("a", self.payload(3))
        cache.put("a", self.payload(1))
        cache.put("b", self.payload(2))
        self.assertIsNotNone(cache.get("a"))
        cache.clear()
        cache.put("c", self.payload(3))
        self.assertIsNotNone(cache.get("c"))
        self.assertIsNone(cache.get("b"))

    def test_snapshot_keeps_truncation_and_drops_connection_promotion(self):
        cache = ListingCache()
        original = self.payload(1)
        cache.put("a", original)
        original["items"].append(9)
        result = cache.get("a")
        self.assertEqual(result["items"], (0,))
        self.assertTrue(result["truncated"])
        self.assertNotIn("promoted", result)
