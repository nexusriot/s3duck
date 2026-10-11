"""What a bucket costs to keep, and what a colder class would cost.

Published AWS us-east-1 list prices. They are an estimate and are labelled as
one wherever they surface -- the point is the ratio between classes, which is
stable, not the absolute figure.
"""


# Indicative us-east-1 list price in USD per GiB-month, storage only. A
# bundled table can never be a bill: prices change, every region differs,
# and the real invoice also carries requests, retrieval and transfer. It
# is enough to answer "is this bucket costing me pennies or hundreds",
# which is the question a size figure alone cannot.
STORAGE_PRICES_USD = {
    "STANDARD": 0.023,
    "REDUCED_REDUNDANCY": 0.024,
    "INTELLIGENT_TIERING": 0.023,
    "STANDARD_IA": 0.0125,
    "ONEZONE_IA": 0.01,
    "GLACIER_IR": 0.004,
    "GLACIER": 0.0036,
    "DEEP_ARCHIVE": 0.00099,
}

# Classes worth suggesting for bytes that are sitting in STANDARD, with
# the catch that makes each one a real decision rather than free money.
COLDER_CLASSES = (
    ("STANDARD_IA", "30-day minimum, per-GB retrieval fee"),
    ("GLACIER_IR", "90-day minimum, per-GB retrieval fee"),
    ("DEEP_ARCHIVE", "180-day minimum, restore takes hours"),
)

PRICING_BASE = 1024 ** 3


def estimate_storage_cost(by_class) -> dict:
    """
    Monthly storage cost for a ``{storage_class: bytes}`` breakdown.

    Returns ``{"total": usd, "by_class": {cls: usd}, "unpriced": [cls]}``.
    A class with no price is reported rather than silently costed at zero.
    """
    priced = {}
    unpriced = []
    total = 0.0
    for name, size in (by_class or {}).items():
        key = str(name or "STANDARD").upper()
        price = STORAGE_PRICES_USD.get(key)
        if price is None:
            unpriced.append(key)
            continue
        cost = (int(size or 0) / PRICING_BASE) * price
        priced[key] = cost
        total += cost
    return {"total": total, "by_class": priced,
            "unpriced": sorted(set(unpriced))}


def colder_class_options(by_class) -> list:
    """
    What the STANDARD bytes would cost in a colder class.

    Returns ``[(class, monthly_usd, monthly_saving, caveat)]``, empty when
    there is nothing in STANDARD to move.
    """
    hot = int((by_class or {}).get("STANDARD", 0) or 0)
    if hot <= 0:
        return []
    gib = hot / PRICING_BASE
    current = gib * STORAGE_PRICES_USD["STANDARD"]
    options = []
    for name, caveat in COLDER_CLASSES:
        cost = gib * STORAGE_PRICES_USD[name]
        options.append((name, cost, current - cost, caveat))
    return options
