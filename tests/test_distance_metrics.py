"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""
import pytest,math
from exercises.b_unit_test_demo.distance_metrics import great_circle_distance


def test_great_circle_distance():
    point_a=(50,10)
    point_b=(78,27)
    assert(great_circle_distance(point_a[0],point_a[1],point_a[0],point_a[1])==0)
    assert(great_circle_distance(point_a[0],point_a[1],point_b[0],point_b[1])==great_circle_distance(point_b[0],point_b[1],point_a[0],point_a[1]))
    assert(great_circle_distance(0,0,180,0,radius=1)==math.pi)
def test_values():
    assert(great_circle_distance(0,0,0,360)==0)
    for point in [(0,90),(0,180),(0,270),(90,0),(180,0),(270,0)]:
        assert(great_circle_distance(0,0,point[0],point[1],radius=1)>0)
    
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
