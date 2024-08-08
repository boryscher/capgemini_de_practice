import numpy as np
from math import ceil


# Function to print output
def print_array(array, msg=''):
    print(' '.join((msg, np.array_str(array))))


# Function to create array
def create_array(new_shape):
    a, b = new_shape
    return np.reshape(np.random.randint(10000, size=a*b), (a, b))


# Function to transpose array
def transpose_array(array):
    a, b = array.shape
    transposed = np.zeros((b, a))
    for i in range(a):
        for j in range(b):
            transposed[j][i] = array[i][j]
    return transposed


# Function to transpose array with numpy built-in
def transpose_array_np(array):
    return np.transpose(array)


# Function to reshape array
def reshape_array(array, new_shape):
    a, b = new_shape
    a1, b1 = array.shape
    if a*b != a1*b1:
        raise ValueError

    series = np.hstack([array[i, :] for i in range(array.shape[0])]).tolist()
    reshaped = np.zeros(new_shape)
    for i in range(a):
        reshaped[i, :] = series[b*i:b*i+b]
    return reshaped


# Function to reshape array with nupy built-in
def reshape_array_np(array, new_shape):
    return np.reshape(array, new_shape)


# Function to split array into chunks
def split_array(array, num_chunks, axis):
    a, b = array.shape
    direction = {0: a, 1: b}
    step = ceil(direction[axis]/num_chunks)
    if axis == 0:
        splitted = [array[step*i:step*i + step, :] for i in range(num_chunks)]
    else:
        splitted = [array[:, step * i:step * i + step] for i in range(num_chunks)]
    return np.array(splitted)


# Function to split array into chunks with numpy built-in
def split_array_np(array, num_chunks, axis):
    return np.array(np.array_split(array, num_chunks, axis))


# Function to combine multiple array into one
def combine_arrays(axis, *arrays):
    if axis == 0:
        return np.vstack(arrays)
    else:
        return np.hstack(arrays)


arr = create_array((6, 8))
print_array(arr, 'Array is created')
print_array(transpose_array(arr), 'Array transposed')
print_array(transpose_array_np(arr), 'Array transposed with numpy buil-in')
print_array(reshape_array(arr, (4, 12)), 'Array reshaped')
print_array(reshape_array_np(arr, (4, 12)), 'Array reshaped with numpy built-in')
print_array(split_array(arr, 4, 1), 'Array splitted into chunks')
print_array(split_array_np(arr, 4, 1), 'Array splitted into chunks with numpy built-in')
arr1 = create_array((6, 6))
arr2 = create_array((6, 6))
arr3 = create_array((6, 6))
print_array(combine_arrays(1, arr1, arr2, arr3), 'Arrays combined into one')
