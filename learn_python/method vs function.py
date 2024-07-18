##difference between function & method

#Method is called by an object, whereas function is independent and can be called directly

str = "abc"
course  = print(str)   #here print is considered a "function" becuase it is universal and can be used to print string/int/float; syntax is 'function(variable)'
course1 = len(str)     #here print is considered a "function" becuase it is universal and can be used to find length of string/int/float; syntax is 'function(variable)'

course2 = print(str.upper()) #-> returns ABC #here upper is considered a "method" because it is constrained and used for converting only STRING object;
                                                #syntax is 'variable.method()' or 'object.method()'
                                                #this STRING object has various methods like upper(), lower(), replace(), find() ....
course3 = print(str.find('b'))#-> returns 2 #here upper is considered a "method" because it is constrained and used for converting only STRING object;
                                                #syntax is 'variable.method('char')' or 'object.method()'

print('d' in str) #-> returns boolean value TRUE/FALSE


#**Method example**

#Ex1:
class abc:
    def abc_method (self):
     print("hello world")

#now abc_method will be called through an object as

abc_object = abc()  # object of abc class
abc_object.abc_method()

#>>> hello world


#Ex2:
import math  #here, math is an imported module; is an object
print(math.ceil(2.9))  #> prints 3 #syntax is object.method()

#**Function example**

a = (1, 2, 3, 4, 5)
x = sum(a)
print(x)

#>>> 20
##added something extra