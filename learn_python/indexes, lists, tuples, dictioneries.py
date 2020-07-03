##indexes##

course = 'python'
print(course[0])  #to get first letter of string
print(course[-1])  #to get last letter of string
print(course[0:3]) #to get first 3 letters of string, that is 1,2,3 characters
print(course[0:])   #if you dont supply end index, python interpreter defaults to lenth of whole string and prints entire string
print(course[:10])   #if you dont supply start index, python interpreter defaults to 0 and prints first 10 characters
print(course[:])   #if you dont supply start and end index, python interpreter defaults both and prints entire string

##formatted string##

first_name = 'rahul'
last_name = 'adepu'
msg= f"{first_name} {last_name} is an employee in our office"


##augmented operator##

step 1: x =10 
step 2: x = x+3 -->13
step2 can aslo be written as:
step 2: x += 3  -->13 #here, '+=' is called augmented operator;here 3 is getting added to previous value 10


##lists##
--list is collection of objects, it is mutable(can add/delete/modify objects in it; ex: names.append('vani') ) and is enclosed in square brackets 
--its values are retieved through indexes

names = ['rahul','reshma','rakesh']
print(names[0])   #prints first value
>> rahul
print(names[:])   #prints all values
>> ['rahul', 'reshma', 'rakesh']


##tuple##
--tuple is collection of objects, it is immutable unlike lists(cannot add/delete/modify objects in i) and is enclosed in circle brackets 

numbers = (6, 7, 8)
print(numbers[0])   #prints first value

##unpacking lists/tuples##

names = ['rahul','reshma','rakesh']

a, b, c = names #interpretor will assign names[0] to a, names[1] to b and names[2] to c

print(a)
>>rahul


##dictionaries##
--dictionary is collection of objects; has key and value pairs defined;  is enclosed in curly brackets 

customers = {"name": "Rahul",
			 "age": 29,
			 "city": "Hyd"}

print(customers["age"])
>>29
