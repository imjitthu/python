import re as validator   
#Inport regular expression module.
#This module provides regular expression matching operations similar to those found in Perl.

user_email = input('Enter a valid email: ')  
#create variable to get email as input from the user

regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'  
#regular expression pattern
  
def check(validate_mail):   #define function to validate email
    if (validator.search(regex, validate_mail)):   
        print("Valid Email")   
    else:   
        print("Invalid Email")   
      
if __name__ == '__main__' :
    check(user_email)   #calling function
