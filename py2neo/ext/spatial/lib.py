from shapely.geometry import MultiPolygon


def parse_poly(lines):
    """ Parse an Osmosis/Google polygon filter file.

    :Params:
        lines : open file pointer

    :Returns:
        shapely.geometry.MultiPolygon object.

    .. note::
        http://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Format

    """
    in_ring = False
    coords = []

    for (index, line) in enumerate(lines):
        if index == 0:
            # ignore meta/garbage.
            continue
        elif index == 1:
            coords.append([[], []])
            ring = coords[-1][0]
            in_ring = True
        elif in_ring and line.strip() == 'END':
            in_ring = False
        elif in_ring:
            ring.append(map(float, line.split()))
        elif not in_ring and line.strip() == 'END':
            break
        elif not in_ring and line.startswith('!'):
            coords[-1][1].append([])
            ring = coords[-1][1][-1]
            in_ring = True
        elif not in_ring:
            coords.append([[], []])
            ring = coords[-1][0]
            in_ring = True

    return MultiPolygon(coords)
