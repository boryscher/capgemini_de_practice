import numpy as np
from math import ceil


# Function to print output
def print_array(array, msg=''):
    print(' '.join((msg, np.array_str(array))))


# Function to create array
def create_array(new_shape):
    a, b = new_shape
    return np.reshape(np.random.randint(10, size=a*b), (a, b))


# Function to save .txt or .csv
def save_array_text(array, filename):
    np.savetxt(filename, array)
    return array


# Function to save .npy
def save_array_binary(array, filename):
    np.save(filename, array)
    return array


# Function to load .txt or .csv
def load_from_text(filename):
    return np.loadtxt(filename)


# Function to load .npy
def load_from_binary(filename):
    return np.load(filename)


# Function to sum elements of array (all array and alongside each axis)
def summation(array, axis=None):
    return np.array(np.sum(array, axis=axis))


# Function to find mean of array (all array and alongside each axis)
def mean_array(array, axis=None):
    return np.array(np.mean(array, axis=axis))


# Function to find median of array (all array and alongside each axis)
def median_array(array, axis=None):
    return np.array((np.median(array, axis=axis)))


# Function to find standard deviation of array (all array and alongside each axis)
def std_array(array, axis=None):
    return np.array(np.std(array, axis=axis))



arr = create_array((10, 10))
print_array(arr, 'This is created array')
print_array((save_array_text(arr, 'save.txt')), 'This array is saved as .txt')
print_array((save_array_text(arr, 'save.csv')), 'This array is saved as .csv')
print_array((save_array_binary(arr, 'save.npy')), 'This array is saved as .npy')
print_array((load_from_text('save.txt')), 'This array is loaded from .txt')
print_array((load_from_text('save.csv')), 'This array is loaded from .csv')
print_array((load_from_binary('save.npy')), 'This array is loaded from .npy')
print_array((summation(arr)), 'This is sum of all elements of array')
print_array((summation(arr, axis=0)), 'This is sum of all elements of array along the rows')
print_array((summation(arr, axis=1)), 'This is sum of all elements of array along the columns')
print_array((mean_array(arr)), 'This is mean value of all elements of array')
print_array((mean_array(arr, axis=0)), 'This is mean value of all elements of array along the rows')
print_array((mean_array(arr, axis=1)), 'This is mean value of all elements of array along the columns')
print_array((median_array(arr)), 'This is median value of all elements of array')
print_array((median_array(arr, axis=0)), 'This is median value of all elements of array along the rows')
print_array((median_array(arr, axis=1)), 'This is median value of all elements of array along the columns')
print_array((std_array(arr)), 'This is standard deviation value of all elements of array')
print_array((std_array(arr, axis=0)), 'This is standard deviation value of all elements of array along the rows')
print_array((std_array(arr, axis=1)), 'This is standard deviation value of all elements of array along the columns')
