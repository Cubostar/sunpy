# -*- coding: utf-8 -*-
"""
Test mapsequence functionality
"""
import numpy as np
import astropy.units as u
import sunpy
import sunpy.map
from sunpy.util.metadata import MetaDict
import pytest
import os
import sunpy.data.test


@pytest.fixture
def aia_map():
    """
    Load SunPy's test AIA image.
    """
    testpath = sunpy.data.test.rootdir
    aia_file = os.path.join(testpath, "aia_171_level1.fits")
    return sunpy.map.Map(aia_file)


@pytest.fixture
def eit_map():
    """
    Load SunPy's test EIT image.
    """
    testpath = sunpy.data.test.rootdir
    eit_file = os.path.join(testpath, "EIT", "efz20040301.020010_s.fits")
    return sunpy.map.Map(eit_file)


@pytest.fixture
def masked_aia_map(aia_map):
    """
    Put a simple mask in the test AIA image.  A rectangular (not square) block
    of True values are included to test that operations on the mask respect how
    the mask is stored.
    """
    aia_map_data = aia_map.data
    aia_map_mask = np.zeros_like(aia_map_data)
    aia_map_mask[0:2, 0:3] = True
    return sunpy.map.Map(np.ma.masked_array(aia_map_data, mask=aia_map_mask),
                         aia_map.meta)


@pytest.fixture
def mapsequence_all_the_same(aia_map):
    """ Simple `sunpy.map.mapsequence` for testing."""
    return sunpy.map.Map([aia_map, aia_map], sequence=True)


@pytest.fixture
def mapsequence_different_maps(aia_map, eit_map):
    """
    Simple `sunpy.map.mapsequence` for testing, in which there are different maps
    """
    return sunpy.map.Map([aia_map, eit_map], sequence=True)


@pytest.fixture
def mapsequence_all_the_same_all_have_masks(masked_aia_map):
    """ Simple `sunpy.map.mapsequence` for testing, in which all the maps have
    masks."""
    return sunpy.map.Map([masked_aia_map, masked_aia_map], sequence=True)


@pytest.fixture
def mapsequence_all_the_same_some_have_masks(aia_map, masked_aia_map):
    """ Simple `sunpy.map.mapsequence` for testing, in which at least some of the
    maps have masks."""
    return sunpy.map.Map([masked_aia_map, masked_aia_map, aia_map], sequence=True)


@pytest.fixture()
def mapsequence_different(aia_map):
    """ Mapsequence allows that the size of the image data in each map be
    different.  This mapsequence contains such maps."""
    return sunpy.map.Map([aia_map, aia_map.superpixel((4, 4) * u.pix)], sequence=True)


def test_all_maps_same_shape(mapsequence_all_the_same, mapsequence_different):
    """Make sure that Mapsequence knows if all the maps have the same shape"""
    assert mapsequence_all_the_same.all_maps_same_shape()
    assert not mapsequence_different.all_maps_same_shape()


def test_at_least_one_map_has_mask(mapsequence_all_the_same,
                                   mapsequence_all_the_same_all_have_masks,
                                   mapsequence_all_the_same_some_have_masks
                                   ):
    """ Test that we can detect the presence of at least one masked map."""
    assert not mapsequence_all_the_same.at_least_one_map_has_mask()
    assert mapsequence_all_the_same_all_have_masks.at_least_one_map_has_mask()
    assert mapsequence_all_the_same_some_have_masks.at_least_one_map_has_mask()


def test_as_array(mapsequence_all_the_same,
                  mapsequence_different,
                  mapsequence_all_the_same_all_have_masks,
                  mapsequence_all_the_same_some_have_masks):
    """Make sure the data in the mapsequence returns correctly, when all the
    maps have the same shape.  When they don't have the same shape, make
    sure an error is raised."""
    # Should raise a ValueError if the mapsequence has differently shaped maps in
    # it.
    with pytest.raises(ValueError):
        mapsequence_different.as_array()

    # Test the case when none of the maps have a mask
    returned_array = mapsequence_all_the_same.as_array()
    assert isinstance(returned_array, np.ndarray)
    assert returned_array.ndim == 3
    assert len(returned_array.shape) == 3
    assert returned_array.shape[0] == 128
    assert returned_array.shape[1] == 128
    assert returned_array.shape[2] == 2
    assert np.ma.getmask(returned_array) is np.ma.nomask

    # Test the case when all the maps have masks
    returned_array = mapsequence_all_the_same_all_have_masks.as_array()
    assert isinstance(returned_array, np.ma.masked_array)
    data = np.ma.getdata(returned_array)
    assert data.ndim == 3
    assert len(data.shape) == 3
    assert data.shape[0] == 128
    assert data.shape[1] == 128
    assert data.shape[2] == 2
    mask = np.ma.getmask(returned_array)
    assert mask.ndim == 3
    assert len(mask.shape) == 3
    assert mask.shape[0] == 128
    assert mask.shape[1] == 128
    assert mask.shape[2] == 2
    assert mask.dtype == bool

    # Test the case when some of the maps have masks
    returned_array = mapsequence_all_the_same_some_have_masks.as_array()
    assert isinstance(returned_array, np.ma.masked_array)
    data = np.ma.getdata(returned_array)
    assert data.ndim == 3
    assert len(data.shape) == 3
    assert data.shape[0] == 128
    assert data.shape[1] == 128
    assert data.shape[2] == 3
    mask = np.ma.getmask(mapsequence_all_the_same_some_have_masks.as_array())
    assert mask.ndim == 3
    assert len(mask.shape) == 3
    assert mask.shape[0] == 128
    assert mask.shape[1] == 128
    assert mask.shape[2] == 3
    assert np.all(mask[0:2, 0:3, 0])
    assert np.all(mask[0:2, 0:3, 1])
    assert np.all(np.logical_not(mask[0:2, 0:3, 2]))


def test_all_meta(mapsequence_all_the_same):
    """Tests that the correct number of map meta objects are returned, and
    that they are all map meta objects."""
    meta = mapsequence_all_the_same.all_meta()
    assert len(meta) == 2
    assert np.all(np.asarray([isinstance(h, MetaDict) for h in meta]))
    assert np.all(np.asarray([meta[i] == mapsequence_all_the_same[i].meta for i in range(0, len(meta))]))


def test_repr(mapsequence_all_the_same, mapsequence_different_maps):
    """
    Tests that overidden __repr__ functionality works as expected. Test
    for mapsequence of same maps as well that of different maps.
    """
    # Test the case of MapSequence having same maps
    expected_out = f'MapSequence of 2 elements, with maps from AIAMap'
    obtained_out = repr(mapsequence_all_the_same)
    assert len(mapsequence_all_the_same) == 2
    assert obtained_out == expected_out

    # Test the case of MapSequence having different maps
    expected_out1 = f'MapSequence of 2 elements, with maps from AIAMap, EITMap'
    expected_out2 = f'MapSequence of 2 elements, with maps from EITMap, AIAMap'
    obtained_out = repr(mapsequence_different_maps)
    assert len(mapsequence_different_maps) == 2
    assert obtained_out == expected_out1 or obtained_out == expected_out2


def test_derotate():
    with pytest.raises(NotImplementedError):
        mapsequence = sunpy.map.MapSequence(derotate=True)
