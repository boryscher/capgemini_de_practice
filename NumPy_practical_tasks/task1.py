import numpy as np


# Function to print output
def print_array(array, msg=''):
    print(' '.join((msg, np.array_str(array))))


# Creates 1d array in given range
def one_dim(num_range):
    return np.arange(1, num_range + 1, dtype='int8')


# Creates 2d array in given range
def two_dim(num_range):
    return np.arange(1, num_range, dtype='int8').reshape(3, 3)


# Gets element from array by index
def get_index(array, ind):
    return np.array(array[ind - 1])


# Creates slice of array with given rectangular size
def create_slice(array, slice_ind):
    return np.array(array[:slice_ind, :slice_ind])


# Adds number to array elements
def add_num(array, num):
    return array + num


# Multiplies array elements on number
def multiply_num(array, num):
    return array * num


print_array(one_dim(10), 'This is 1d array')
print_array(two_dim(10), 'This is 2d array')
print_array(get_index(one_dim(10), 3), 'This is 3rd element of 1d array')
print_array(create_slice(two_dim(10), 2), 'This is 2 first rows and columns slice')
print_array(add_num(one_dim(10), 5), 'Add 5 to 1d array elements')
print_array(multiply_num(two_dim(10), 2), 'Multiply 2d array elements by 2')

