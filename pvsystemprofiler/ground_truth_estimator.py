import urllib3 as urllib2
from pykml import parser
import numpy as np
from haversine import haversine
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import os

def ground_truth_finder(subdir):
    def readCoordinates(filename):

        file = filename

        #root = parser.fromstring(open(file,'r').read())

        with open(file, 'rb') as f:
            root = parser.fromstring(f.read())
            #root_read = f.read()

        # Read coordinates from KML file
        coordinates = root.Document.Placemark.Polygon.outerBoundaryIs.LinearRing.coordinates
        s = coordinates.__str__()
        s = s.split()

        # Extract coordinates
        j = 0
        for i in s:
            if j==1:
                c1 = np.fromstring(i,dtype=np.float,sep=',')
            elif j==2:
                c2 = np.fromstring(i,dtype=np.float,sep=',')
            elif j==3:
                c3 = np.fromstring(i,dtype=np.float,sep=',')
            j = j+1

        return c1,c2,c3

    def coordinatesToCartesian(c1,c2,c3):
        # Convert coordinates to Cartesian
        temp1 = (c2[1],c1[0])
        temp2 = (c3[1],c1[0])
        c1z = c1[2]/1000.0
        c1 = (c1[1],c1[0])
        c2z = c2[2]/1000.0
        c2 = (c2[1],c2[0])
        c3z = c3[2]/1000.0
        c3 = (c3[1],c3[0])
        y1 = haversine(c1, temp1)
        x1 = haversine(c2, temp1)
        y2 = haversine(c1, temp2)
        x2 = haversine(c3, temp2)
        p1 = np.array([0, 0, c1z])
        p2 = np.array([x1, y1, c2z])
        p3 = np.array([x2, y2, c3z])

        return p1,p2,p3

    def normalVec(p1,p2,p3):
        # These two vectors are in the plane
        v1 = p2 - p1
        v2 = p3 - p1

        # The cross product is a vector normal to the plane
        cp = np.cross(v1, v2)
        x, y, z = cp

        #Ensure vector is in the positive direction
        if z<0:
            z = abs(z)
            x = 0-x
            y = 0-y

        return x,y,z

    def tiltAz(x,y,z):
        # Estimate tilt and azimuth
        tilt = np.rad2deg(np.arctan2(np.sqrt((x*x)+(y*y))*(180/np.pi), z*(180/np.pi)))
        azimuth = -90-np.rad2deg(np.arctan2(y*(180/np.pi), x*(180/np.pi)))
        if azimuth < -180:
            azimuth = 360 + azimuth

        return tilt,azimuth

    dire = os.path.join(os.getcwd(), 'roofs')
    subdirs = [x[0] for x in os.walk(dire)]
    subdirs.sort()

    aa = []
    bb = []
    cc = []
    files = os.walk(subdir).__next__()[2]
    length = len(files)
    if length>0:
        files.sort()
        tilt = np.zeros(length)
        azimuth = np.zeros(length)
        i = 0
        for filen in files: #for each .kml of the same roof
            if not filen.startswith('.'):
                os.chdir(subdir)

                #read coordinates
                c1,c2,c3 = readCoordinates(filen)

                #convert coordinates to cartesian
                p1,p2,p3 = coordinatesToCartesian(c1,c2,c3)

                #get the normal vector to the plane
                x,y,z = normalVec(p1,p2,p3)
                aa.append(x)
                bb.append(y)
                cc.append(z)
                t,a = tiltAz(x,y,z)

                #gather all tilts and azimuths of same home
                tilt[i] = t
                azimuth[i] = a
                i = i + 1

        #calculate mean and standard deviation tilt and azimuth per home
        ave_tilt = np.mean(tilt)
        std_tilt = np.std(tilt)
        med_tilt = np.median(tilt)
        ave_azimuth = np.mean(azimuth)
        std_azimuth = np.std(azimuth)
        med_azimuth = np.median(azimuth)
        return ave_tilt, med_tilt, ave_azimuth, med_azimuth

# subdir = '/Users/elpiniki/Documents/SLAC/PVInsight/notebooks/tiltazimuth/roofs/TAEAC1006600'
# a,b,c,d = ground_truth_finder(subdir)
# print (a,b,c,d)
