"""Squarified treemap layout, and the prefix tree it draws.

The layout is plain arithmetic on a unit rectangle; the widget that paints it
supplies the pixels.
"""


def build_prefix_tree(entries, separator="/") -> dict:
    """
    Nest ``(key, size)`` pairs into ``{name: {size, count, children}}``.

    The usage pie answers "what kind of files"; this answers "which folder",
    which is the one a bill is actually made of.
    """
    root = {"size": 0, "count": 0, "children": {}}
    for key, size in entries or []:
        text = str(key or "")
        if not text or text.endswith(separator):
            continue
        size = int(size or 0)
        node = root
        node["size"] += size
        node["count"] += 1
        parts = [part for part in text.split(separator) if part]
        for part in parts[:-1]:
            node = node["children"].setdefault(
                part, {"size": 0, "count": 0, "children": {}})
            node["size"] += size
            node["count"] += 1
        if parts:
            leaf = node["children"].setdefault(
                parts[-1], {"size": 0, "count": 0, "children": {}})
            leaf["size"] += size
            leaf["count"] += 1
    return root
def node_at_path(root, path) -> dict:
    """Walk a prefix tree to ``path`` (a list of names), or {} if it is gone."""
    node = root or {}
    for part in path or []:
        node = (node.get("children") or {}).get(part)
        if node is None:
            return {}
    return node
def treemap_children(node, limit=80) -> list:
    """
    ``[(name, size, is_group)]`` for one level, biggest first.

    Everything past the limit is folded into one "other" tile: a thousand
    slivers is not a picture of anything.
    """
    rows = sorted(((name, child["size"])
                   for name, child in (node.get("children") or {}).items()),
                  key=lambda row: row[1], reverse=True)
    rows = [row for row in rows if row[1] > 0]
    if len(rows) <= limit:
        return [(name, size, False) for name, size in rows]
    head = [(name, size, False) for name, size in rows[:limit]]
    rest = sum(size for _name, size in rows[limit:])
    if rest:
        head.append((f"… {len(rows) - limit} more", rest, True))
    return head
def _treemap_row(sizes, x, y, dx, dy) -> list:
    """Lay one run of areas along the shorter side of the box."""
    covered = sum(sizes)
    rects = []
    if covered <= 0:
        return rects
    if dx >= dy:
        width = covered / dy if dy else 0
        for size in sizes:
            height = size / width if width else 0
            rects.append((x, y, width, height))
            y += height
    else:
        height = covered / dx if dx else 0
        for size in sizes:
            width = size / height if height else 0
            rects.append((x, y, width, height))
            x += width
    return rects
def _treemap_worst(sizes, dx, dy) -> float:
    """The worst aspect ratio a run would produce; lower is squarer."""
    rects = _treemap_row(sizes, 0, 0, dx, dy)
    worst = 0.0
    for _x, _y, width, height in rects:
        if width <= 0 or height <= 0:
            return float("inf")
        worst = max(worst, width / height, height / width)
    return worst
def squarify(values, x=0.0, y=0.0, width=1.0, height=1.0) -> list:
    """
    Squarified treemap rectangles for *values*, in the same order.

    Iterative rather than recursive: a bucket with thousands of prefixes would
    otherwise recurse once per row.
    """
    values = [float(value) for value in values or []]
    if not values or width <= 0 or height <= 0:
        return []
    total = sum(values)
    if total <= 0:
        return []
    scale = (width * height) / total
    areas = [value * scale for value in values]

    rects = []
    index = 0
    while index < len(areas):
        run = 1
        while (index + run < len(areas)
               and _treemap_worst(areas[index:index + run], width, height)
               >= _treemap_worst(areas[index:index + run + 1], width, height)):
            run += 1
        current = areas[index:index + run]
        rects.extend(_treemap_row(current, x, y, width, height))
        covered = sum(current)
        if width >= height:
            used = covered / height if height else 0
            x += used
            width -= used
        else:
            used = covered / width if width else 0
            y += used
            height -= used
        index += run
        if width <= 0 or height <= 0:
            break
    return rects
