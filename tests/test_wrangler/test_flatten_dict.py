import pytest
from MetaWrangler import MetaWrangler


def test_flatten_dict_leaves_count():
    def count(d):
        a, b = 0, 0  # subdicts and not-subdicts
        for key, value in d.items():
            if isinstance(value, dict):
                a += 1
                suba, subb = count(value)
                a += suba
                b += subb
            else:
                b += 1
        return a, b

    wrangler = MetaWrangler()
    input_dict = wrangler.con.Jobs.GetJob("6639a543ee72d7dfc75d8178")

    flattened_dict = wrangler.flatten_dict(input_dict)

    _, original_values_count = count(input_dict)
    flattened_values_count = sum(1 for v in flattened_dict.values() if not isinstance(v, dict))
    assert original_values_count == flattened_values_count