##indexes
course = 'python'
print(course[0])  #to get first letter of string
print(course[-1])  #to get last letter of string
print(course[0:3]) #to get first 3 letters of string, that is 1,2,3 characters
print(course[0:])   #if you dont supply end index, python interpreter defaults to lenth of whole string and prints entire string
print(course[:10])   #if you dont supply start index, python interpreter defaults to 0 and prints first 10 characters
print(course[:1])   #if you dont supply start and end index, python interpreter defaults both and prints entire string

##formatted string
first_name = 'rahul'
last_name = 'adepu'
msg= f"{first_name} {last_name} is an employee in our office"


##difference between function & method

--Method is called by an object, whereas function is independent and can be called directly

course  = print("abc")   #here print is considered a "function" becuase it is universal and can be used to print string/int/float; syntax is 'function(variable)'
course1 = len("abc")     #here print is considered a "function" becuase it is universal and can be used to find length of string/int/float; syntax is 'function(variable)'

course2 = print(course.upper())---> returns ABC #here upper is considered a "method" because it is constrained and used for converting only STRING object;
												#syntax is 'variable.method()' or 'object.method()'
												#this STRING object has various methods like upper(), lower(), replace(), find() ....
course3 = print(course.find('b'))---> returns 2 #here upper is considered a "method" because it is constrained and used for converting only STRING object; 
												#syntax is 'variable.method('char')' or 'object.method()'

print('d' in course) ---> returns boolean value TRUE/FALSE


**Method example**

Ex1:
class abc:
	def abc_method (self):
	 print("hello world")

#now abc_method will be called through an object as

abc_object = abc()  # object of abc class
abc_object.abc_method

>>> hello world


Ex2:
import math  #here, math is an imported module; is an object
print(math.ceil(2.9))  --> prints 3 #syntax is object.method()

**Function example**
	 
s = sum(5, 15)  #here sum is an inbuilt-function
print( s ) 

>>> 20

##augmented operator

step 1: x =10 
step 2: x = x+3 -->13
step2 can aslo be written as:
step 2: x += 3  -->13 #here, '+=' is called augmented operator;here 3 is getting added to previous value 10

##method 

