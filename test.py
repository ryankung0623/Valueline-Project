# a function that returns all the prime numbers between a and b
def prime_numbers(a, b):
    # a list of prime numbers
    prime_list = []
    # a list of numbers to check if they are prime
    check_list = []
    # a list of numbers to check if they are prime
    for i in range(a, b):
        check_list.append(i)
    # a list of numbers to check if they are prime
    for i in range(2, b):
        if i in check_list:
            prime_list.append(i)
            for j in range(i, b, i):
                if j in check_list:
                    check_list.remove(j)
    return prime_list

for prime in prime_numbers(0, 100):
    print(prime)