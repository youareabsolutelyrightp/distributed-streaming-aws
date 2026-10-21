import random
import unittest

from taxi_reference.ranking import ProfitEntry, RouteEntry, TopTen
from taxi_reference.windows import ExactMedianWindow


class MedianTests(unittest.TestCase):
    def test_even_odd_and_duplicate_removal(self):
        window = ExactMedianWindow()
        window.add(100)
        self.assertEqual(window.median_twice_cents(), 200)
        window.add(200)
        self.assertEqual(window.median_twice_cents(), 300)
        window.add(200)
        self.assertEqual(window.median_twice_cents(), 400)
        window.add(300)
        self.assertEqual(window.median_twice_cents(), 400)
        window.remove(200)
        self.assertEqual(window.median_twice_cents(), 400)
        window.remove(100)
        self.assertEqual(window.median_twice_cents(), 500)
        window.remove(200)
        self.assertEqual(window.median_twice_cents(), 600)
        window.remove(300)
        self.assertEqual(len(window), 0)

    def test_randomized_against_sorted_values(self):
        random_source = random.Random(20260803)
        window = ExactMedianWindow()
        values = []
        for _ in range(2000):
            if values and random_source.random() < 0.48:
                position = random_source.randrange(len(values))
                value = values.pop(position)
                window.remove(value)
            else:
                value = random_source.randrange(0, 500)
                values.append(value)
                window.add(value)
            self.assertEqual(len(window), len(values))
            if values:
                ordered = sorted(values)
                middle = len(ordered) // 2
                expected = (
                    ordered[middle] * 2
                    if len(ordered) % 2
                    else ordered[middle - 1] + ordered[middle]
                )
                self.assertEqual(window.median_twice_cents(), expected)


class RankingTests(unittest.TestCase):
    def test_route_tie_breaks(self):
        ranking = TopTen[RouteEntry]()
        ranking.set(RouteEntry(2, 1, 100, 1))
        ranking.set(RouteEntry(1, 1, 100, 1))
        self.assertEqual([entry.key for entry in ranking.top], [1, 2])
        ranking.set(RouteEntry(2, 2, 100, 2))
        self.assertEqual([entry.key for entry in ranking.top], [2, 1])

    def test_profit_uses_exact_rational_ranking(self):
        ranking = TopTen[ProfitEntry]()
        ranking.set(ProfitEntry(1, 1, 3, 100, 1))
        ranking.set(ProfitEntry(2, 2, 7, 100, 2))
        self.assertEqual([entry.key for entry in ranking.top], [1, 2])
        ranking.set(ProfitEntry(3, 2, 6, 100, 3))
        self.assertEqual([entry.key for entry in ranking.top], [3, 1, 2])


if __name__ == "__main__":
    unittest.main()
