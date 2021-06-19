i = 10
j = 15
ij = i +j +5
print(i)
print(j)
print(ij)

#Dynamic Variables
name = 'rex'
name = 'jon'
name = name + ' rex'
print(name)

#Basic Maths
total = (1 + 10 / 3 * 100 - 0.9)
total = 5//2
total = 3**2
total = 5%2
print(total)

#if condition
input = int(input("enter the number: ")) #converted string to integer
if input == 40:
    print(f'input is valid' + str(input))
elif input >= 40:
    print(f'input is ' + str(input) + ' not valid') #converted integer to string
else:
    print(f'input '  + str(input) + ' is not valid') #converted integer to string
    
print (range(input)) #print the range of given value
print (list(range(input))) #print the values in given range
print (list(range(input+1))) #adding 1 value into the range
