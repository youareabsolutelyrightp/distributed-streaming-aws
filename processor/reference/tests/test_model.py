import unittest

from taxi_reference.model import (
    CENTER_LATITUDE,
    CENTER_LONGITUDE,
    Q1_LATITUDE_STEP,
    Q1_LONGITUDE_STEP,
    Q2_LATITUDE_STEP,
    Q2_LONGITUDE_STEP,
    cell_text,
    grid_cell,
    parse_cents,
    parse_decimal_nano,
    round_half_even_ratio,
)


class GridTests(unittest.TestCase):
    def assert_grid_edges(self, size: int, longitude_step: int, latitude_step: int):
        west = CENTER_LONGITUDE - longitude_step // 2
        north = CENTER_LATITUDE + latitude_step // 2
        self.assertEqual(
            cell_text(
                grid_cell(
                    west,
                    north,
                    size=size,
                    longitude_step=longitude_step,
                    latitude_step=latitude_step,
                )
            ),
            "1.1",
        )
        self.assertEqual(
            cell_text(
                grid_cell(
                    west + longitude_step,
                    north,
                    size=size,
                    longitude_step=longitude_step,
                    latitude_step=latitude_step,
                )
            ),
            "2.1",
        )
        self.assertEqual(
            cell_text(
                grid_cell(
                    west,
                    north - latitude_step,
                    size=size,
                    longitude_step=longitude_step,
                    latitude_step=latitude_step,
                )
            ),
            "1.2",
        )
        self.assertIsNone(
            grid_cell(
                west - 1,
                north,
                size=size,
                longitude_step=longitude_step,
                latitude_step=latitude_step,
            )
        )
        self.assertIsNone(
            grid_cell(
                west,
                north + 1,
                size=size,
                longitude_step=longitude_step,
                latitude_step=latitude_step,
            )
        )
        self.assertEqual(
            cell_text(
                grid_cell(
                    west + size * longitude_step - 1,
                    north - size * latitude_step + 1,
                    size=size,
                    longitude_step=longitude_step,
                    latitude_step=latitude_step,
                )
            ),
            f"{size}.{size}",
        )
        self.assertIsNone(
            grid_cell(
                west + size * longitude_step,
                north,
                size=size,
                longitude_step=longitude_step,
                latitude_step=latitude_step,
            )
        )
        self.assertIsNone(
            grid_cell(
                west,
                north - size * latitude_step,
                size=size,
                longitude_step=longitude_step,
                latitude_step=latitude_step,
            )
        )

    def test_query1_edges(self):
        self.assert_grid_edges(300, Q1_LONGITUDE_STEP, Q1_LATITUDE_STEP)

    def test_query2_edges(self):
        self.assert_grid_edges(600, Q2_LONGITUDE_STEP, Q2_LATITUDE_STEP)

    def test_exact_decimal_parser(self):
        self.assertEqual(parse_decimal_nano("-74.913585"), CENTER_LONGITUDE)
        self.assertEqual(parse_decimal_nano("41.474937000"), CENTER_LATITUDE)
        self.assertIsNone(parse_decimal_nano("41.1234567891"))


class MoneyTests(unittest.TestCase):
    def test_canonical_cents(self):
        self.assertEqual(parse_cents("3.5"), 350)
        self.assertEqual(parse_cents("-0.01"), -1)
        self.assertEqual(parse_cents("+2"), 200)
        self.assertIsNone(parse_cents("1.005"))

    def test_half_even_six_places(self):
        self.assertEqual(round_half_even_ratio(1, 2_000_000, 6), "0.000000")
        self.assertEqual(round_half_even_ratio(3, 2_000_000, 6), "0.000002")
        self.assertEqual(round_half_even_ratio(701, 200, 6), "3.505000")


if __name__ == "__main__":
    unittest.main()
