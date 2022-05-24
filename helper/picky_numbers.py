import numpy as np

def isfloat(number):
    try:
        float(number)
        return True
    except ValueError:
        return False

def picky_mean(numbers: list) -> float:
    # make a new list containing only floats in numbers list 
    new_numbers = [float(number) for number in numbers if isfloat(number)]
    
    if len(new_numbers) > 0:
        return np.mean(new_numbers)
    else:
        return np.nan

def convert_to_float(number: str) -> float:
    if isfloat(number):
        return float(number)
    else:
        return np.nan
    


