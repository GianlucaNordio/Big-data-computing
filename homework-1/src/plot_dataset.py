import matplotlib.pyplot as plt
import numpy as np

# List of points
points = [(0.4,0.9), (0.5,4.1), (0.8,0.91), (0.81,1.1), (1.1,5.0), (1.11,5.1), (1.5,1.1), (1.52,1.11), (1.53,1.12), (1.54,1.13), (1.51,3.2), (1.52,3.6), (3.21,4.6), (4.11,4.11), (4.32,4.3)]  # replace with your points
radius = 1  # replace with your radius

fig, ax = plt.subplots()

# Plot each point and circle
for point in points:
    circle = plt.Circle((point[0], point[1]), radius, fill=False)
    ax.add_artist(circle)
    ax.plot(point[0], point[1], 'bo')  # plot the point

# Set equal aspect ratio
ax.set_aspect('equal', adjustable='datalim')

plt.show()