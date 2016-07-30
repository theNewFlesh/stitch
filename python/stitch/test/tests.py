from __future__ import with_statement, print_function, absolute_import
from itertools import *
from functools import *
import os
from stitch.core.stitch_frame import StitchFrame
from stitch.core.stitch_string import StitchString
# ------------------------------------------------------------------------------

_YAML = os.path.abspath('./resources/stitch_string.yml')
_JSON = dict(
    a1=dict(
        b1=dict(
            c1=1,
            c2=2,
            c3=3
        )
    ),
    a2=dict(
        b2=dict(
            c4=list('abcdef'),
            c5=[
                dict(
                    d1='a',
                    d2='b'
                ),
                dict(
                    d3='c',
                    d4='d'
                )
            ],
            c6=[
                [1,2,3],
                [4,5,6],
                [7,8,9]
            ]
        )
    ),
    a3='b3'
)
_JSON = [_JSON]
_JSON = [_JSON, _JSON, _JSON]

_DATA = [
    [1,2,3],
    [4,5,6],
    [7,8,9]
]
# ------------------------------------------------------------------------------

def frame_applymap_001_test():
    data = StitchFrame(_DATA)\
        .applymap(lambda x: 'test', columns=[0])\
        .to_dataframe().loc[0, 0]
    assert(data == 'test')

def frame_flatten_001_test():
    data = StitchFrame(_JSON)\
        .flatten(prefix=False)\
        .flatten()\
        .flatten()\
        .flatten('a2_b2_c4', dtype=list)\
        .flatten('a2_b2_c5', prefix=False)\
        .flatten([0,1], prefix=False)\
        .to_dataframe().ix[0].T.tolist()
    assert(data == [1, 2, 3, 'a', 'b', 'c', 'd', 'e', 'f', 'a', 'b', 'c', 'd',
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]], 'b3']
    )

def string_parse_001_test():
    st = StitchString(_YAML)
    string = 'sceneHOUSE100.shot42_layer05.v001.exr'
    response = st.parse(string)
    assert(response == {
        'layer': 'layer05',
        'shot': 'shot42',
        'extension': 'exr',
        'scene': 'HOUSE100',
        'ver_null': '',
        'version': '001'
        }
    )

def string_parse_002_test():
    st = StitchString(_YAML)
    string = 'sceneHOUSE100.shot42_layer05.v001.jpg'
    response = st.parse(string)
    assert(response == {
        'extension': 'jpg',
        'layer': 'layer05',
        'scene': 'HOUSE100',
        'shot': 'shot42',
        'ver_null': '',
        'version': '001'
        }
    )

def string_parse_003_test():
    st = StitchString(_YAML)
    string = 'sceneHOUSE100.layer05_shot42.v001.exr'
    response = st.parse(string)
    assert(response == {
        'extension': 'exr',
        'layer': 'layer05',
        'scene': 'HOUSE100',
        'shot': 'shot42',
        'ver_null': '',
        'version': '001'
        }
    )

def string_parse_004_test():
    st = StitchString(_YAML)
    string = 'sceneHOUSE101.layer05_shot42.v001.jpg'
    response = st.parse(string)
    assert(response == {
        'extension': 'jpg',
        'layer': 'layer05',
        'scene': 'HOUSE101',
        'shot': 'shot42',
        'ver_null': '',
        'version': '001'
        }
    )

def string_parse_005_test():
    st = StitchString(_YAML)
    string = 'layer05_shot42-sceneHOUSE100.v001.jpg'
    response = st.parse(string)
    assert(response == {
        'extension': 'jpg',
        'layer': 'layer05',
        'scene': 'HOUSE100',
        'shot': 'shot42',
        'ver_null': '',
        'version': '001'
        }
    )

def string_parse_006_test():
    st = StitchString(_YAML)
    string = 'layer05_shot42-scnHOUSE100.v001.jpg'
    response = st.parse(string)
    assert(response == {
        'extension': 'jpg',
        'layer': 'layer05',
        'scene': 'HOUSE100',
        'shot': 'shot42',
        'ver_null': '',
        'version': '001'
        }
    )
# ------------------------------------------------------------------------------

def main():
    help(__main__)
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
