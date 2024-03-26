import matplotlib.pyplot as plt
from matplotlib.patches import Circle

def plot_points_from_file(file_path, D):
    x_values = []
    y_values = []

    with open(file_path, 'r') as file:
        for line in file:
            # Splitting line by comma to get x and y coordinates
            coordinates = line.strip().split(',')
            if len(coordinates) == 2:
                x = float(coordinates[0])
                y = float(coordinates[1])
                x_values.append(x)
                y_values.append(y)
                # Plotting circle for each point
                circle = Circle((x, y), D, color='r', fill=False)
                plt.gca().add_patch(circle)

    # Plotting points
    plt.scatter(x_values, y_values, color='b', marker='o')

    # Plotting grid
    # Finding the bounds for the grid
    min_x = min(x_values) - D
    max_x = max(x_values) + D
    min_y = min(y_values) - D
    max_y = max(y_values) + D

    side = D / (2 * (2 ** 0.5))

    min_grid_x = int(min_x / side) - 1
    min_grid_y = int(min_y / side) - 1
    max_grid_x = int(max_x / side) + 1
    max_grid_y = int(max_y / side) + 1

    # Plotting grid within the bounds of the points
    for x in range(min_grid_x, max_grid_x + 1):
        for y in range(min_grid_y, max_grid_y + 1):
            if x != max_grid_x:
                plt.plot([x * side, (x + 1) * side], [y * side, y * side], color='gray', linestyle='-', linewidth=0.5)
            if y != max_grid_y:
                plt.plot([x * side, x * side], [y * side, (y + 1) * side], color='gray', linestyle='-', linewidth=0.5)


    plt.xlabel('X Coordinate')
    plt.ylabel('Y Coordinate')
    plt.title('Plot of Points with Circles and Grid')
    plt.axis('square')  # Equal aspect ratio
    plt.show()

if __name__ == "__main__":
    file_path = "../input/TestN15-input.txt"
    D = 1
    plot_points_from_file(file_path, D)
