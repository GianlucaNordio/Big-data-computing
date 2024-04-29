file_path = "../input/uber-10k.csv"
points = []
with open(file_path, 'r') as file:
    for line in file:
        # Splitting line by comma to get x and y coordinates
        coordinates = line.strip().split(',')
        if len(coordinates) == 2:
            x = float(coordinates[0])
            y = float(coordinates[1])
            points.append((x,y))

print("Number of points", len(points))
print("Number of unique points", len(set(points)))